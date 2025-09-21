use clap::{Arg, ArgAction, Command};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};
use libc::ENOENT;
use rusqlite::{Connection, Result};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const TTL: Duration = Duration::from_secs(1); // 1 second

/// # Unified Notes Schema FUSE Filesystem
/// 
/// This filesystem implements a unified schema where both files and folders are represented
/// as notes in a single `notes` table. The distinction between files and folders is determined
/// dynamically based on whether a note has children.
/// 
/// ## Schema Structure
/// ```sql
/// CREATE TABLE notes (
///     id TEXT PRIMARY KEY,
///     title TEXT NOT NULL,
///     abstract TEXT,              -- NOTE: Not used by FUSE filesystem
///     content TEXT NOT NULL,
///     syntax TEXT NOT NULL DEFAULT 'markdown',
///     parent_id TEXT,
///     user_id TEXT NOT NULL,
///     created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
///     updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
///     FOREIGN KEY (parent_id) REFERENCES notes(id) ON DELETE CASCADE
/// );
/// ```
/// 
/// ## File/Folder Behavior
/// 
/// ### Files (Leaf Notes)
/// - A note with **no children** is presented as a **file**
/// - Filename: `{title}.{extension}` (extension based on `syntax` field)
/// - Content: Direct mapping from the `content` field
/// - Example: Note titled "Python Basics" → `Python Basics.md`
/// 
/// ### Folders (Parent Notes)
/// - A note with **children** is presented as a **directory**
/// - Directory name: `{title}` (no extension)
/// - The note's own content is accessible as `{title}/index.{extension}`
/// - Child notes appear as files/subdirectories within this directory
/// 
/// ### Dynamic Transformation Example
/// ```
/// Initial state:
/// - Note: "Python" (content: "Python programming guide") → File: `Python.md`
/// 
/// After adding child "Pandas":
/// - Note: "Python" → Directory: `Python/`
/// - Python's content → File: `Python/index.md`
/// - Child note: "Pandas" → File: `Python/Pandas.md`
/// ```
/// 
/// ## Implementation Notes
/// - Root directory has `parent_id = NULL`
/// - File extensions determined by `syntax` field (markdown → .md, etc.)
/// - Filesystem operations must handle dynamic file↔folder transitions
/// - Index files (`index.{ext}`) provide access to parent note content
/// - Ordering by `updated_at DESC` for conflict resolution
struct SqliteFS {
    db: Connection,
    inode_map: HashMap<String, u64>,
    reverse_inode_map: HashMap<u64, String>,
    next_inode: u64,
}

impl SqliteFS {
    fn new(db_path: &str) -> Result<Self> {
        let db = Connection::open(db_path)?;

        // Create performance indexes for unified notes table
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_notes_parent_title ON notes(parent_id, title)",
            [],
        )?;
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_notes_parent_updated ON notes(parent_id, updated_at DESC)",
            [],
        )?;
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_notes_parent_id ON notes(parent_id)",
            [],
        )?;
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_notes_user_id ON notes(user_id)",
            [],
        )?;

        let mut fs = SqliteFS {
            db,
            inode_map: HashMap::new(),
            reverse_inode_map: HashMap::new(),
            next_inode: 2,
        };

        // Root directory gets inode 1
        fs.inode_map.insert("/".to_string(), 1);
        fs.reverse_inode_map.insert(1, "/".to_string());

        Ok(fs)
    }

    fn get_or_create_inode(&mut self, path: &str) -> u64 {
        if let Some(&inode) = self.inode_map.get(path) {
            return inode;
        }

        let inode = self.next_inode;
        self.next_inode += 1;
        self.inode_map.insert(path.to_string(), inode);
        self.reverse_inode_map.insert(inode, path.to_string());
        inode
    }

    fn get_path_from_inode(&self, inode: u64) -> Option<&String> {
        self.reverse_inode_map.get(&inode)
    }

    fn get_parent_folder_id(&self, parent_path: &str) -> Result<String> {
        if parent_path == "/" {
            // Root directory - NULL parent_id
            return Ok("".to_string());
        }

        // Split the path and find the folder ID by walking through the hierarchy
        let path_parts: Vec<&str> = parent_path.trim_start_matches('/').split('/').collect();
        let mut current_parent_id: Option<String> = None;

        for part in path_parts {
            if part.is_empty() {
                continue;
            }

            // Find the folder with this title under current_parent_id
            // In the new schema, folders are represented as notes without content
            let folder_id: String = self.db.query_row(
                "SELECT id FROM notes WHERE parent_id IS ?1 AND title = ?2 ORDER BY updated_at DESC LIMIT 1",
                rusqlite::params![current_parent_id.as_deref(), part],
                |row| row.get(0)
            )?;

            current_parent_id = Some(folder_id);
        }

        Ok(current_parent_id.unwrap_or_default())
    }

    fn strip_md_suffix(filename: &str) -> &str {
        filename.strip_suffix(".md").unwrap_or(filename)
    }

    fn add_md_suffix(title: &str) -> String {
        if title.ends_with(".md") {
            title.to_string()
        } else {
            format!("{title}.md")
        }
    }

    /// Generate a UUID v4 string for database record IDs
    fn generate_uuid() -> String {
        Uuid::new_v4().to_string()
    }

    /// Create a new note in the database (unified schema)
    ///
    /// In the new unified schema, both files and folders are represented as notes.
    /// A note becomes a folder when it has children, and its content is accessible via index.{ext}.
    ///
    /// Arguments:
    /// - parent_path: Filesystem path of the parent directory (e.g., "/Projects")
    /// - title: Title of the note (without file extension)
    /// - content: Content to store in the note's content field
    /// - syntax: Syntax type (e.g., "markdown", "text", "code")
    /// - user_id: ID of the user creating the note
    ///
    /// Returns:
    /// - Ok(String): UUID of the newly created note
    /// - Err: Database error if insertion fails
    fn create_note(&mut self, parent_path: &str, title: &str, content: &str, syntax: &str, user_id: &str) -> Result<String> {
        // Get the parent note ID (could be None for root level)
        let parent_id = if parent_path == "/" {
            None
        } else {
            Some(self.get_parent_folder_id(parent_path)?)
        };

        // Generate new UUID for the note
        let note_id = Self::generate_uuid();

        // Get current timestamp as string (matching the existing data format)
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        // Insert new note into database
        // Note: abstract field is not used by FUSE filesystem, left empty
        self.db.execute(
            "INSERT INTO notes (id, title, abstract, content, syntax, parent_id, user_id, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![&note_id, title, "", content, syntax, parent_id, user_id, &now, &now],
        )?;

        Ok(note_id)
    }

    /// Helper function to get file extension from syntax
    fn get_extension_from_syntax(syntax: &str) -> &str {
        match syntax {
            "markdown" => "md",
            "python" => "py",
            "javascript" => "js",
            "typescript" => "ts",
            "rust" => "rs",
            "html" => "html",
            "css" => "css",
            "json" => "json",
            "yaml" => "yml",
            "xml" => "xml",
            _ => "txt",
        }
    }

    /// Helper function to get syntax from file extension
    fn get_syntax_from_extension(extension: &str) -> &str {
        match extension {
            "md" => "markdown",
            "py" => "python", 
            "js" => "javascript",
            "ts" => "typescript",
            "rs" => "rust",
            "html" => "html",
            "css" => "css",
            "json" => "json",
            "yml" | "yaml" => "yaml",
            "xml" => "xml",
            _ => "text",
        }
    }
}

impl Filesystem for SqliteFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Construct full path
        let full_path = if parent_path == "/" {
            format!("/{name_str}")
        } else {
            format!("{parent_path}/{name_str}")
        };

        // Get parent note ID (None for root level)
        let parent_note_id = if parent_path == "/" {
            None
        } else {
            match self.get_parent_folder_id(&parent_path) {
                Ok(id) => Some(id),
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Handle special case for index files (index.{ext})
        if name_str.starts_with("index.") {
            if let Some(parent_id) = &parent_note_id {
                // Look up the parent note to get its content via index file
                if let Ok(note_result) = self.db.query_row(
                    "SELECT id, title, content, syntax, created_at, updated_at FROM notes WHERE id = ?1",
                    [parent_id],
                    |row| {
                        let id: String = row.get(0)?;
                        let content: String = row.get(2)?;
                        let syntax: String = row.get(3)?;
                        let created_at: String = row.get(4)?;
                        let updated_at: String = row.get(5)?;
                        Ok((id, content, syntax, created_at, updated_at))
                    }
                ) {
                    let expected_ext = Self::get_extension_from_syntax(&note_result.2);
                    let expected_index = format!("index.{}", expected_ext);
                    
                    if name_str == expected_index {
                        let inode = self.get_or_create_inode(&full_path);
                        let content_size = note_result.1.len();
                        
                        // TODO: Parse timestamp strings properly
                        let attr = FileAttr {
                            ino: inode,
                            size: content_size as u64,
                            blocks: content_size.div_ceil(512) as u64,
                            atime: UNIX_EPOCH,  // TODO: Parse created_at
                            mtime: UNIX_EPOCH,  // TODO: Parse updated_at
                            ctime: UNIX_EPOCH,  // TODO: Parse updated_at
                            crtime: UNIX_EPOCH, // TODO: Parse created_at
                            kind: FileType::RegularFile,
                            perm: 0o644,
                            nlink: 1,
                            uid: 501,
                            gid: 20,
                            rdev: 0,
                            flags: 0,
                            blksize: 512,
                        };
                        reply.entry(&TTL, &attr, 0);
                        return;
                    }
                }
            }
        }

        // Look for a note with matching title (unified schema)
        let note_query = "SELECT id, title, content, syntax, created_at, updated_at FROM notes WHERE parent_id IS ?1 AND title = ?2 ORDER BY updated_at DESC LIMIT 1";
        
        // Try exact title match first (for directories)
        if let Ok(note_result) = self.db.query_row(
            note_query,
            rusqlite::params![parent_note_id, name_str],
            |row| {
                let id: String = row.get(0)?;
                let content: String = row.get(2)?;
                let syntax: String = row.get(3)?;
                let created_at: String = row.get(4)?;
                let updated_at: String = row.get(5)?;
                Ok((id, content, syntax, created_at, updated_at))
            }
        ) {
            // Check if this note has children (making it a directory)
            let has_children = self.db.query_row(
                "SELECT COUNT(*) FROM notes WHERE parent_id = ?1",
                [&note_result.0],
                |row| row.get::<_, i64>(0)
            ).unwrap_or(0) > 0;

            if has_children {
                // This note has children, so it's a directory
                let inode = self.get_or_create_inode(&full_path);
                let attr = FileAttr {
                    ino: inode,
                    size: 0,
                    blocks: 0,
                    atime: UNIX_EPOCH,  // TODO: Parse created_at
                    mtime: UNIX_EPOCH,  // TODO: Parse updated_at
                    ctime: UNIX_EPOCH,  // TODO: Parse updated_at
                    crtime: UNIX_EPOCH, // TODO: Parse created_at
                    kind: FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: 501,
                    gid: 20,
                    rdev: 0,
                    flags: 0,
                    blksize: 512,
                };
                reply.entry(&TTL, &attr, 0);
                return;
            }
        }

        // Try stripping file extension and matching title (for files)
        if let Some(dot_pos) = name_str.rfind('.') {
            let title_without_ext = &name_str[..dot_pos];
            let requested_ext = &name_str[dot_pos + 1..];
            
            if let Ok(note_result) = self.db.query_row(
                note_query,
                rusqlite::params![parent_note_id, title_without_ext],
                |row| {
                    let id: String = row.get(0)?;
                    let content: String = row.get(2)?;
                    let syntax: String = row.get(3)?;
                    let created_at: String = row.get(4)?;
                    let updated_at: String = row.get(5)?;
                    Ok((id, content, syntax, created_at, updated_at))
                }
            ) {
                // Check if this note has children
                let has_children = self.db.query_row(
                    "SELECT COUNT(*) FROM notes WHERE parent_id = ?1",
                    [&note_result.0],
                    |row| row.get::<_, i64>(0)
                ).unwrap_or(0) > 0;

                if !has_children {
                    // This note has no children, so it's a file
                    // Verify the extension matches the syntax
                    let expected_ext = Self::get_extension_from_syntax(&note_result.2);
                    if requested_ext == expected_ext {
                        let inode = self.get_or_create_inode(&full_path);
                        let content_size = note_result.1.len();
                        
                        let attr = FileAttr {
                            ino: inode,
                            size: content_size as u64,
                            blocks: content_size.div_ceil(512) as u64,
                            atime: UNIX_EPOCH,  // TODO: Parse created_at
                            mtime: UNIX_EPOCH,  // TODO: Parse updated_at
                            ctime: UNIX_EPOCH,  // TODO: Parse updated_at
                            crtime: UNIX_EPOCH, // TODO: Parse created_at
                            kind: FileType::RegularFile,
                            perm: 0o644,
                            nlink: 1,
                            uid: 501,
                            gid: 20,
                            rdev: 0,
                            flags: 0,
                            blksize: 512,
                        };
                        reply.entry(&TTL, &attr, 0);
                        return;
                    }
                }
            }
        }

        reply.error(ENOENT);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if ino == 1 {
            // Root directory
            let attr = FileAttr {
                ino: 1,
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };
            reply.attr(&TTL, &attr);
            return;
        }

        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Extract the filename and parent path
        let (parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", &path[..])
        };

        // Get parent note ID (None for root level)
        let parent_note_id = if parent_path == "/" {
            None
        } else {
            match self.get_parent_folder_id(parent_path) {
                Ok(id) => Some(id),
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Handle special case for index files (index.{ext})
        if filename.starts_with("index.") {
            if let Some(parent_id) = &parent_note_id {
                // Look up the parent note to get its content via index file
                if let Ok(note_result) = self.db.query_row(
                    "SELECT id, title, content, syntax, created_at, updated_at FROM notes WHERE id = ?1",
                    [parent_id],
                    |row| {
                        let content: String = row.get(2)?;
                        let syntax: String = row.get(3)?;
                        let created_at: String = row.get(4)?;
                        let updated_at: String = row.get(5)?;
                        Ok((content, syntax, created_at, updated_at))
                    }
                ) {
                    let expected_ext = Self::get_extension_from_syntax(&note_result.1);
                    let expected_index = format!("index.{}", expected_ext);
                    
                    if filename == expected_index {
                        let content_size = note_result.0.len();
                        let attr = FileAttr {
                            ino,
                            size: content_size as u64,
                            blocks: content_size.div_ceil(512) as u64,
                            atime: UNIX_EPOCH,  // TODO: Parse created_at
                            mtime: UNIX_EPOCH,  // TODO: Parse updated_at
                            ctime: UNIX_EPOCH,  // TODO: Parse updated_at
                            crtime: UNIX_EPOCH, // TODO: Parse created_at
                            kind: FileType::RegularFile,
                            perm: 0o644,
                            nlink: 1,
                            uid: 501,
                            gid: 20,
                            rdev: 0,
                            flags: 0,
                            blksize: 512,
                        };
                        reply.attr(&TTL, &attr);
                        return;
                    }
                }
            }
        }

        // Query database for notes (unified schema)
        let note_query = "SELECT id, title, content, syntax, created_at, updated_at FROM notes WHERE parent_id IS ?1 AND title = ?2 ORDER BY updated_at DESC LIMIT 1";
        
        // Try exact title match first (for directories)
        if let Ok(note_result) = self.db.query_row(
            note_query,
            rusqlite::params![parent_note_id, filename],
            |row| {
                let id: String = row.get(0)?;
                let content: String = row.get(2)?;
                let syntax: String = row.get(3)?;
                let created_at: String = row.get(4)?;
                let updated_at: String = row.get(5)?;
                Ok((id, content, syntax, created_at, updated_at))
            }
        ) {
            // Check if this note has children (making it a directory)
            let has_children = self.db.query_row(
                "SELECT COUNT(*) FROM notes WHERE parent_id = ?1",
                [&note_result.0],
                |row| row.get::<_, i64>(0)
            ).unwrap_or(0) > 0;

            if has_children {
                // This note has children, so it's a directory
                let attr = FileAttr {
                    ino,
                    size: 0,
                    blocks: 0,
                    atime: UNIX_EPOCH,  // TODO: Parse created_at
                    mtime: UNIX_EPOCH,  // TODO: Parse updated_at
                    ctime: UNIX_EPOCH,  // TODO: Parse updated_at
                    crtime: UNIX_EPOCH, // TODO: Parse created_at
                    kind: FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: 501,
                    gid: 20,
                    rdev: 0,
                    flags: 0,
                    blksize: 512,
                };
                reply.attr(&TTL, &attr);
                return;
            }
        }

        // Try stripping file extension and matching title (for files)
        if let Some(dot_pos) = filename.rfind('.') {
            let title_without_ext = &filename[..dot_pos];
            let requested_ext = &filename[dot_pos + 1..];
            
            if let Ok(note_result) = self.db.query_row(
                note_query,
                rusqlite::params![parent_note_id, title_without_ext],
                |row| {
                    let id: String = row.get(0)?;
                    let content: String = row.get(2)?;
                    let syntax: String = row.get(3)?;
                    let created_at: String = row.get(4)?;
                    let updated_at: String = row.get(5)?;
                    Ok((id, content, syntax, created_at, updated_at))
                }
            ) {
                // Check if this note has children
                let has_children = self.db.query_row(
                    "SELECT COUNT(*) FROM notes WHERE parent_id = ?1",
                    [&note_result.0],
                    |row| row.get::<_, i64>(0)
                ).unwrap_or(0) > 0;

                if !has_children {
                    // This note has no children, so it's a file
                    // Verify the extension matches the syntax
                    let expected_ext = Self::get_extension_from_syntax(&note_result.2);
                    if requested_ext == expected_ext {
                        let content_size = note_result.1.len();
                        let attr = FileAttr {
                            ino,
                            size: content_size as u64,
                            blocks: content_size.div_ceil(512) as u64,
                            atime: UNIX_EPOCH,  // TODO: Parse created_at
                            mtime: UNIX_EPOCH,  // TODO: Parse updated_at
                            ctime: UNIX_EPOCH,  // TODO: Parse updated_at
                            crtime: UNIX_EPOCH, // TODO: Parse created_at
                            kind: FileType::RegularFile,
                            perm: 0o644,
                            nlink: 1,
                            uid: 501,
                            gid: 20,
                            rdev: 0,
                            flags: 0,
                            blksize: 512,
                        };
                        reply.attr(&TTL, &attr);
                        return;
                    }
                }
            }
        }

        reply.error(ENOENT);
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Extract the filename and parent path
        let (parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", &path[..])
        };

        // Get parent note ID (None for root level)
        let parent_note_id = if parent_path == "/" {
            None
        } else {
            match self.get_parent_folder_id(parent_path) {
                Ok(id) => Some(id),
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        // Handle special case for index files (index.{ext})
        if filename.starts_with("index.") {
            if let Some(parent_id) = &parent_note_id {
                // Look up the parent note to get its content via index file
                if let Ok(content) = self.db.query_row(
                    "SELECT content, syntax FROM notes WHERE id = ?1",
                    [parent_id],
                    |row| {
                        let content: String = row.get(0)?;
                        let syntax: String = row.get(1)?;
                        Ok((content, syntax))
                    }
                ) {
                    let expected_ext = Self::get_extension_from_syntax(&content.1);
                    let expected_index = format!("index.{}", expected_ext);
                    
                    if filename == expected_index {
                        let content_bytes = content.0.as_bytes();
                        let start = offset as usize;
                        if start < content_bytes.len() {
                            reply.data(&content_bytes[start..]);
                        } else {
                            reply.data(&[]);
                        }
                        return;
                    }
                }
            }
        }

        // Query database for note content (unified schema)
        let note_query = "SELECT id, content, syntax FROM notes WHERE parent_id IS ?1 AND title = ?2 ORDER BY updated_at DESC LIMIT 1";
        
        // Try stripping file extension and matching title (for files)
        if let Some(dot_pos) = filename.rfind('.') {
            let title_without_ext = &filename[..dot_pos];
            let requested_ext = &filename[dot_pos + 1..];
            
            if let Ok(note_result) = self.db.query_row(
                note_query,
                rusqlite::params![parent_note_id, title_without_ext],
                |row| {
                    let id: String = row.get(0)?;
                    let content: String = row.get(1)?;
                    let syntax: String = row.get(2)?;
                    Ok((id, content, syntax))
                }
            ) {
                // Check if this note has children (should be a file for reading)
                let has_children = self.db.query_row(
                    "SELECT COUNT(*) FROM notes WHERE parent_id = ?1",
                    [&note_result.0],
                    |row| row.get::<_, i64>(0)
                ).unwrap_or(0) > 0;

                if !has_children {
                    // This note has no children, so it's a file
                    // Verify the extension matches the syntax
                    let expected_ext = Self::get_extension_from_syntax(&note_result.2);
                    if requested_ext == expected_ext {
                        let content_bytes = note_result.1.as_bytes();
                        let start = offset as usize;
                        if start < content_bytes.len() {
                            reply.data(&content_bytes[start..]);
                        } else {
                            reply.data(&[]);
                        }
                        return;
                    }
                }
            }
        }

        reply.error(ENOENT);
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (1, FileType::Directory, "..".to_string()),
        ];

        // Get the parent folder ID for this directory
        let parent_folder_id = match self.get_parent_folder_id(&path) {
            Ok(id) => id,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        // Query folders
        let folder_query = "SELECT id, title FROM folders WHERE parent_id = ?1 AND deleted_time = 0 ORDER BY user_updated_time DESC";
        let folder_titles = {
            if let Ok(mut stmt) = self.db.prepare(folder_query) {
                if let Ok(rows) = stmt.query_map([&parent_folder_id], |row| {
                    let title: String = row.get(1)?;
                    Ok(title)
                }) {
                    let mut titles = Vec::new();
                    for row in rows {
                        if let Ok(title) = row {
                            titles.push(title);
                        }
                    }
                    titles
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        };

        for title in folder_titles {
            let full_path = if path == "/" {
                format!("/{title}")
            } else {
                format!("{path}/{title}")
            };
            let inode = self.get_or_create_inode(&full_path);
            entries.push((inode, FileType::Directory, title));
        }

        // Query notes
        let note_query = "SELECT id, title FROM notes WHERE parent_id = ?1 AND deleted_time = 0 ORDER BY user_updated_time DESC";
        let note_titles = {
            if let Ok(mut stmt) = self.db.prepare(note_query) {
                if let Ok(rows) = stmt.query_map([&parent_folder_id], |row| {
                    let title: String = row.get(1)?;
                    Ok(title)
                }) {
                    let mut titles = Vec::new();
                    for row in rows {
                        if let Ok(title) = row {
                            titles.push(title);
                        }
                    }
                    titles
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        };

        for title in note_titles {
            // Add .md suffix to note titles for filesystem display
            let display_title = Self::add_md_suffix(&title);
            let full_path = if path == "/" {
                format!("/{display_title}")
            } else {
                format!("{path}/{display_title}")
            };
            let inode = self.get_or_create_inode(&full_path);
            entries.push((inode, FileType::RegularFile, display_title));
        }

        // Handle path conflicts - if there are duplicate titles, favor the most recent based on user_updated_time
        let mut seen_titles = std::collections::HashSet::new();
        let mut unique_entries = Vec::new();

        for entry in entries {
            if entry.2 == "." || entry.2 == ".." {
                unique_entries.push(entry);
            } else if !seen_titles.contains(&entry.2) {
                seen_titles.insert(entry.2.clone());
                unique_entries.push(entry);
            }
        }

        for (i, entry) in unique_entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(entry.0, (i + 1) as i64, entry.1, &entry.2) {
                break;
            }
        }
        reply.ok();
    }

    /// Handle directory creation operations (unified schema)
    /// 
    /// In the unified schema, creating a directory means creating a note that will act as a folder.
    /// The note is created with empty content initially, and if children are added later,
    /// its content becomes accessible via index.{ext}.
    ///
    /// Key behaviors:
    /// - Creates a note in the database that represents a directory
    /// - Uses default syntax "markdown" for new directories
    /// - TODO: Need user_id - for now using placeholder
    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let folder_name = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Create the note/folder in the database with empty content
        // TODO: Get actual user_id from request or configuration
        let user_id = "default_user"; // Placeholder
        
        match self.create_note(&parent_path, folder_name, "", "markdown", user_id) {
            Ok(_note_id) => {
                // Create the full path for the new directory
                let full_path = if parent_path == "/" {
                    format!("/{folder_name}")
                } else {
                    format!("{parent_path}/{folder_name}")
                };

                // Create inode for the new directory
                let inode = self.get_or_create_inode(&full_path);

                // Get current timestamp for attributes
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                let attr = FileAttr {
                    ino: inode,
                    size: 0,
                    blocks: 0,
                    atime: UNIX_EPOCH + Duration::from_secs(now),
                    mtime: UNIX_EPOCH + Duration::from_secs(now),
                    ctime: UNIX_EPOCH + Duration::from_secs(now),
                    crtime: UNIX_EPOCH + Duration::from_secs(now),
                    kind: FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: 501,
                    gid: 20,
                    rdev: 0,
                    flags: 0,
                    blksize: 512,
                };

                reply.entry(&TTL, &attr, 0);
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    /// Handle file creation operations (unified schema)
    /// 
    /// Creates a new note in the database. The file extension determines the syntax type.
    /// In the unified schema, this creates a note that will be presented as a file until
    /// it gets children (at which point it becomes a directory with index.{ext} for content).
    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let file_name = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Extract title and syntax from filename
        let (title, syntax) = if let Some(dot_pos) = file_name.rfind('.') {
            let title = &file_name[..dot_pos];
            let extension = &file_name[dot_pos + 1..];
            let syntax = Self::get_syntax_from_extension(extension);
            (title, syntax)
        } else {
            // No extension, default to text
            (file_name, "text")
        };

        // TODO: Get actual user_id from request or configuration
        let user_id = "default_user"; // Placeholder

        // Create the note in the database with empty content initially
        match self.create_note(&parent_path, title, "", syntax, user_id) {
            Ok(_note_id) => {
                // Create the full path for the new file
                let full_path = if parent_path == "/" {
                    format!("/{file_name}")
                } else {
                    format!("{parent_path}/{file_name}")
                };

                // Create inode for the new file
                let inode = self.get_or_create_inode(&full_path);

                // Get current timestamp for attributes
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                let attr = FileAttr {
                    ino: inode,
                    size: 0, // Empty file initially
                    blocks: 0,
                    atime: UNIX_EPOCH + Duration::from_secs(now),
                    mtime: UNIX_EPOCH + Duration::from_secs(now),
                    ctime: UNIX_EPOCH + Duration::from_secs(now),
                    crtime: UNIX_EPOCH + Duration::from_secs(now),
                    kind: FileType::RegularFile,
                    perm: 0o644,
                    nlink: 1,
                    uid: 501,
                    gid: 20,
                    rdev: 0,
                    flags: 0,
                    blksize: 512,
                };

                // Return the created file with a file handle (using inode as fh)
                reply.created(&TTL, &attr, 0, inode, 0);
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    /// Handle file write operations
    /// This method is called when applications write data to open files.
    /// It supports both overwriting (offset 0) and appending/inserting at specific offsets.
    /// The content is immediately written to the database's 'body' field.
    ///
    /// Key behaviors:
    /// - offset 0: Completely overwrites existing content
    /// - offset > 0: Inserts/appends data at the specified position
    /// - Updates timestamps (updated_time, user_updated_time) in database
    /// - Strips .md suffix when looking up notes in database
    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Extract the filename and parent path
        let (parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", &path[..])
        };

        // Get the parent folder ID and strip .md suffix for database lookup
        let parent_folder_id = match self.get_parent_folder_id(parent_path) {
            Ok(id) => id,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        let db_title = Self::strip_md_suffix(filename);

        // Get the current content of the note
        let current_content = match self.db.query_row(
            "SELECT body FROM notes WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0 ORDER BY user_updated_time DESC LIMIT 1",
            [&parent_folder_id, db_title],
            |row| row.get::<_, String>(0)
        ) {
            Ok(content) => content,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        // Handle the write operation
        let new_content = if offset == 0 {
            // Overwrite from the beginning
            String::from_utf8_lossy(data).to_string()
        } else {
            // Append or insert at offset
            let mut content_bytes = current_content.into_bytes();
            let start_pos = offset as usize;

            if start_pos > content_bytes.len() {
                // If offset is beyond current content, pad with zeros
                content_bytes.resize(start_pos, 0);
            }

            // Replace or extend content
            if start_pos + data.len() <= content_bytes.len() {
                // Replace existing content
                content_bytes[start_pos..start_pos + data.len()].copy_from_slice(data);
            } else {
                // Extend content
                content_bytes.truncate(start_pos);
                content_bytes.extend_from_slice(data);
            }

            String::from_utf8_lossy(&content_bytes).to_string()
        };

        // Update the note in the database
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        match self.db.execute(
            "UPDATE notes SET body = ?1, updated_time = ?2, user_updated_time = ?3 WHERE parent_id = ?4 AND title = ?5 AND deleted_time = 0",
            [&new_content, &now.to_string(), &now.to_string(), &parent_folder_id, db_title],
        ) {
            Ok(_) => {
                reply.written(data.len() as u32);
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    /// Handle file opening operations
    /// This method is called when editors or applications use open() system call
    /// to open existing files for reading or writing
    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        // Verify that the inode exists and corresponds to a valid file
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Extract the filename and parent path for database verification
        let (parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", &path[..])
        };

        // Verify the file exists in the database
        let parent_folder_id = match self.get_parent_folder_id(parent_path) {
            Ok(id) => id,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        let db_title = Self::strip_md_suffix(filename);

        // Check if the note exists in the database
        let note_exists = self.db.query_row(
            "SELECT 1 FROM notes WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0 ORDER BY user_updated_time DESC LIMIT 1",
            [&parent_folder_id, db_title],
            |_| Ok(true)
        ).unwrap_or(false);

        if note_exists {
            // File exists, return success with the inode as file handle
            // Using the inode as file handle simplifies file handle management
            reply.opened(ino, 0);
        } else {
            // File doesn't exist in database
            reply.error(ENOENT);
        }
    }

    /// Handle file attribute setting operations
    /// This method is called when editors or applications try to set file attributes
    /// such as timestamps, file size, permissions, etc. Many editors require this
    /// operation to function properly.
    ///
    /// Key behaviors:
    /// - Handles size changes (truncation/extension of file content)
    /// - Updates timestamps in the database when modified
    /// - Validates that the file exists before making changes
    /// - Returns updated file attributes after successful changes
    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        // Get the file path from inode
        let path = match self.get_path_from_inode(ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Extract the filename and parent path for database operations
        let (parent_path, filename) = if let Some(pos) = path.rfind('/') {
            let parent = &path[..pos];
            let name = &path[pos + 1..];
            (if parent.is_empty() { "/" } else { parent }, name)
        } else {
            ("/", &path[..])
        };

        // Get the parent folder ID and strip .md suffix for database lookup
        let parent_folder_id = match self.get_parent_folder_id(parent_path) {
            Ok(id) => id,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        let db_title = Self::strip_md_suffix(filename);

        // Handle size changes (file truncation/extension)
        if let Some(new_size) = size {
            // Get current content to modify its size
            let current_content = match self.db.query_row(
                "SELECT body FROM notes WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0 ORDER BY user_updated_time DESC LIMIT 1",
                [&parent_folder_id, db_title],
                |row| row.get::<_, String>(0)
            ) {
                Ok(content) => content,
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            };

            let mut content_bytes = current_content.into_bytes();
            let current_size = content_bytes.len();
            let target_size = new_size as usize;

            // Adjust content size based on target
            if target_size < current_size {
                // Truncate content
                content_bytes.truncate(target_size);
            } else if target_size > current_size {
                // Extend content with null bytes
                content_bytes.resize(target_size, 0);
            }

            let new_content = String::from_utf8_lossy(&content_bytes).to_string();

            // Update content in database
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            if let Err(_) = self.db.execute(
                "UPDATE notes SET body = ?1, updated_time = ?2, user_updated_time = ?3 WHERE parent_id = ?4 AND title = ?5 AND deleted_time = 0",
                [&new_content, &now.to_string(), &now.to_string(), &parent_folder_id, db_title],
            ) {
                reply.error(libc::EIO);
                return;
            }
        }

        // Get current file information for returning updated attributes
        let (content_size, created_time, updated_time) = match self.db.query_row(
            "SELECT body, created_time, updated_time FROM notes WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0 ORDER BY user_updated_time DESC LIMIT 1",
            [&parent_folder_id, db_title],
            |row| {
                let body: String = row.get(0)?;
                let created: i64 = row.get(1)?;
                let updated: i64 = row.get(2)?;
                Ok((body.len(), created, updated))
            }
        ) {
            Ok(data) => data,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        // Return updated file attributes
        let attr = FileAttr {
            ino,
            size: content_size as u64,
            blocks: content_size.div_ceil(512) as u64,
            atime: UNIX_EPOCH + Duration::from_secs(created_time as u64),
            mtime: UNIX_EPOCH + Duration::from_secs(updated_time as u64),
            ctime: UNIX_EPOCH + Duration::from_secs(updated_time as u64),
            crtime: UNIX_EPOCH + Duration::from_secs(created_time as u64),
            kind: FileType::RegularFile,
            perm: mode.unwrap_or(0o644) as u16,
            nlink: 1,
            uid: uid.unwrap_or(501),
            gid: gid.unwrap_or(20),
            rdev: 0,
            flags: 0,
            blksize: 512,
        };

        reply.attr(&TTL, &attr);
    }

    /// Handle file flush operations
    /// This method is called when editors or applications want to ensure that
    /// all pending writes have been completed. Since we write directly to the
    /// database in our write() method, this is essentially a no-op, but we
    /// need to implement it for editor compatibility.
    ///
    /// Key behaviors:
    /// - Always returns success since writes are already persistent
    /// - Required for proper editor functionality (many editors call flush before close)
    /// - Validates that the file handle corresponds to a valid file
    fn flush(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        // Verify that the inode exists (basic validation)
        if self.get_path_from_inode(ino).is_some() {
            // Since we write directly to the database, flush is always successful
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    /// Handle file release (close) operations
    /// This method is called when a file handle is closed. Since we don't
    /// maintain any file-specific state or resources, this is essentially
    /// a no-op, but it's required for proper FUSE operation.
    ///
    /// Key behaviors:
    /// - Always returns success since no cleanup is needed
    /// - Called when editors close files or when file handles are released
    /// - Validates that the file handle corresponds to a valid file
    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        // Verify that the inode exists (basic validation)
        if self.get_path_from_inode(ino).is_some() {
            // No cleanup needed since we don't maintain file-specific resources
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    /// Handle file and directory renaming operations
    /// This method is called when a file or directory is renamed (e.g., using mv command).
    /// It updates the database to reflect the new name while preserving all other metadata.
    ///
    /// Key behaviors:
    /// - Updates the 'title' field in the database for the renamed item
    /// - Handles both files (notes) and directories (folders)
    /// - Strips .md suffix from filenames before storing in database
    /// - Updates the user_updated_time timestamp
    /// - Maintains proper parent-child relationships
    /// - Required for proper file manager and shell integration
    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let old_name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let new_name = match newname.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // Get parent paths
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let new_parent_path = match self.get_path_from_inode(newparent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Get parent folder IDs from database
        let parent_folder_id = match self.get_parent_folder_id(&parent_path) {
            Ok(id) => id,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        let new_parent_folder_id = match self.get_parent_folder_id(&new_parent_path) {
            Ok(id) => id,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Try to rename as a file first (strip .md suffix for database)
        let old_title = Self::strip_md_suffix(old_name);
        let new_title = Self::strip_md_suffix(new_name);

        let file_result = self.db.execute(
            "UPDATE notes SET title = ?1, parent_id = ?2, user_updated_time = ?3 WHERE parent_id = ?4 AND title = ?5 AND deleted_time = 0",
            [new_title, &new_parent_folder_id, &current_time.to_string(), &parent_folder_id, old_title]
        );

        if let Ok(rows_affected) = file_result {
            if rows_affected > 0 {
                // Successfully renamed a file
                // Update inode mappings
                let old_path = if parent_path == "/" {
                    format!("/{old_name}")
                } else {
                    format!("{parent_path}/{old_name}")
                };

                let new_path = if new_parent_path == "/" {
                    format!("/{new_name}")
                } else {
                    format!("{new_parent_path}/{new_name}")
                };

                // Update inode mappings
                if let Some(inode) = self.inode_map.remove(&old_path) {
                    self.inode_map.insert(new_path.clone(), inode);
                    self.reverse_inode_map.insert(inode, new_path);
                }

                reply.ok();
                return;
            }
        }

        // Try to rename as a folder
        let folder_result = self.db.execute(
            "UPDATE folders SET title = ?1, parent_id = ?2, user_updated_time = ?3 WHERE parent_id = ?4 AND title = ?5 AND deleted_time = 0",
            [new_name, &new_parent_folder_id, &current_time.to_string(), &parent_folder_id, old_name]
        );

        if let Ok(rows_affected) = folder_result {
            if rows_affected > 0 {
                // Successfully renamed a folder
                // Update inode mappings
                let old_path = if parent_path == "/" {
                    format!("/{old_name}")
                } else {
                    format!("{parent_path}/{old_name}")
                };

                let new_path = if new_parent_path == "/" {
                    format!("/{new_name}")
                } else {
                    format!("{new_parent_path}/{new_name}")
                };

                // Update inode mappings for the folder and all its descendants
                let mut paths_to_update = Vec::new();
                for (path, inode) in &self.inode_map {
                    if path.starts_with(&old_path) {
                        let new_descendant_path = path.replacen(&old_path, &new_path, 1);
                        paths_to_update.push((path.clone(), new_descendant_path, *inode));
                    }
                }

                for (old_path, new_path, inode) in paths_to_update {
                    self.inode_map.remove(&old_path);
                    self.inode_map.insert(new_path.clone(), inode);
                    self.reverse_inode_map.insert(inode, new_path);
                }

                reply.ok();
                return;
            }
        }

        // Neither file nor folder was found
        reply.error(ENOENT);
    }

    /// Handle file deletion operations
    /// This method is called when a file is deleted (e.g., using rm command).
    /// It removes the corresponding row from the notes table in the database.
    ///
    /// Key behaviors:
    /// - Deletes the most recent row (based on user_updated_time) if duplicates exist
    /// - Strips .md suffix from filename before database lookup
    /// - Updates inode mappings to reflect the deletion
    /// - Required for proper file manager and shell integration
    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let filename = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Get parent folder ID from database
        let parent_folder_id = match self.get_parent_folder_id(&parent_path) {
            Ok(id) => id,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        // Strip .md suffix for database lookup
        let title = Self::strip_md_suffix(filename);

        // Delete the note with the most recent user_updated_time
        let result = self.db.execute(
            "DELETE FROM notes WHERE id = (
                SELECT id FROM notes
                WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0
                ORDER BY user_updated_time DESC
                LIMIT 1
            )",
            [&parent_folder_id, title],
        );

        match result {
            Ok(rows_affected) => {
                if rows_affected > 0 {
                    // Successfully deleted the file
                    // Remove from inode mappings
                    let file_path = if parent_path == "/" {
                        format!("/{filename}")
                    } else {
                        format!("{parent_path}/{filename}")
                    };

                    if let Some(inode) = self.inode_map.remove(&file_path) {
                        self.reverse_inode_map.remove(&inode);
                    }

                    reply.ok();
                } else {
                    // File not found
                    reply.error(ENOENT);
                }
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    /// Handle directory deletion operations
    /// This method is called when a directory is deleted (e.g., using rmdir command).
    /// It removes the corresponding row from the folders table in the database.
    ///
    /// Key behaviors:
    /// - Only deletes empty directories (standard rmdir behavior)
    /// - Deletes the most recent row (based on user_updated_time) if duplicates exist
    /// - Updates inode mappings to reflect the deletion
    /// - Required for proper file manager and shell integration
    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let dirname = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // Get parent path
        let parent_path = match self.get_path_from_inode(parent) {
            Some(path) => path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Get parent folder ID from database
        let parent_folder_id = match self.get_parent_folder_id(&parent_path) {
            Ok(id) => id,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        // First, get the folder ID that we want to delete
        let folder_to_delete_id: Result<String, rusqlite::Error> = self.db.query_row(
            "SELECT id FROM folders
             WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0
             ORDER BY user_updated_time DESC
             LIMIT 1",
            [&parent_folder_id, dirname],
            |row| row.get(0),
        );

        let folder_id = match folder_to_delete_id {
            Ok(id) => id,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        // Check if the directory is empty (no child folders or notes)
        let child_folders: Result<i64, rusqlite::Error> = self.db.query_row(
            "SELECT COUNT(*) FROM folders WHERE parent_id = ?1 AND deleted_time = 0",
            [&folder_id],
            |row| row.get(0),
        );

        let child_notes: Result<i64, rusqlite::Error> = self.db.query_row(
            "SELECT COUNT(*) FROM notes WHERE parent_id = ?1 AND deleted_time = 0",
            [&folder_id],
            |row| row.get(0),
        );

        match (child_folders, child_notes) {
            (Ok(folder_count), Ok(note_count)) => {
                if folder_count > 0 || note_count > 0 {
                    // Directory is not empty
                    reply.error(libc::ENOTEMPTY);
                    return;
                }
            }
            _ => {
                reply.error(libc::EIO);
                return;
            }
        }

        // Directory is empty, proceed with deletion
        let result = self
            .db
            .execute("DELETE FROM folders WHERE id = ?1", [&folder_id]);

        match result {
            Ok(rows_affected) => {
                if rows_affected > 0 {
                    // Successfully deleted the directory
                    // Remove from inode mappings
                    let dir_path = if parent_path == "/" {
                        format!("/{dirname}")
                    } else {
                        format!("{parent_path}/{dirname}")
                    };

                    if let Some(inode) = self.inode_map.remove(&dir_path) {
                        self.reverse_inode_map.remove(&inode);
                    }

                    reply.ok();
                } else {
                    // Should not happen since we just queried for it
                    reply.error(ENOENT);
                }
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }
}

fn main() {
    let matches = Command::new("sqlite_fuse")
        .author("Ryan Greenup")
        .arg(
            Arg::new("DATABASE")
                .required(true)
                .index(1)
                .help("Path to the SQLite database file"),
        )
        .arg(
            Arg::new("MOUNT_POINT")
                .required(true)
                .index(2)
                .help("Act as a client, and mount FUSE at given path"),
        )
        .arg(
            Arg::new("auto_unmount")
                .long("auto_unmount")
                .action(ArgAction::SetTrue)
                .help("Automatically unmount on process exit"),
        )
        .arg(
            Arg::new("allow-root")
                .long("allow-root")
                .action(ArgAction::SetTrue)
                .help("Allow root user to access filesystem"),
        )
        .get_matches();
    env_logger::init();

    let database_path = matches.get_one::<String>("DATABASE").unwrap();
    let mountpoint = matches.get_one::<String>("MOUNT_POINT").unwrap();

    let fs = match SqliteFS::new(database_path) {
        Ok(fs) => fs,
        Err(e) => {
            eprintln!("Failed to open database: {e}");
            std::process::exit(1);
        }
    };

    let mut options = vec![MountOption::FSName("sqlite_fuse".to_string())];
    if matches.get_flag("auto_unmount") {
        options.push(MountOption::AutoUnmount);
    }
    if matches.get_flag("allow-root") {
        options.push(MountOption::AllowRoot);
    }
    fuser::mount2(fs, mountpoint, &options).unwrap();
}
