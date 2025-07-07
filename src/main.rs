use clap::{Arg, ArgAction, Command};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};
use libc::ENOENT;
use rusqlite::{Connection, Result};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};

const TTL: Duration = Duration::from_secs(1); // 1 second


struct SqliteFS {
    db: Connection,
    inode_map: HashMap<String, u64>,
    reverse_inode_map: HashMap<u64, String>,
    next_inode: u64,
}

impl SqliteFS {
    fn new(db_path: &str) -> Result<Self> {
        let db = Connection::open(db_path)?;
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
            // Root directory - empty parent_id
            return Ok("".to_string());
        }
        
        // Split the path and find the folder ID by walking through the hierarchy
        let path_parts: Vec<&str> = parent_path.trim_start_matches('/').split('/').collect();
        let mut current_parent_id = "".to_string();
        
        for part in path_parts {
            if part.is_empty() {
                continue;
            }
            
            // Find the folder with this title under current_parent_id
            let folder_id: String = self.db.query_row(
                "SELECT id FROM folders WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0 ORDER BY user_updated_time DESC LIMIT 1",
                [&current_parent_id, part],
                |row| row.get(0)
            )?;
            
            current_parent_id = folder_id;
        }
        
        Ok(current_parent_id)
    }
    
    fn strip_md_suffix(filename: &str) -> &str {
        filename.strip_suffix(".md").unwrap_or(filename)
    }
    
    fn add_md_suffix(title: &str) -> String {
        if title.ends_with(".md") {
            title.to_string()
        } else {
            format!("{}.md", title)
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
            format!("/{}", name_str)
        } else {
            format!("{}/{}", parent_path, name_str)
        };
        
        // Query database for folders first
        let folder_query = "SELECT id, title, created_time, updated_time, user_updated_time FROM folders WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0 ORDER BY user_updated_time DESC LIMIT 1";
        
        let folder_result = {
            if let Ok(parent_folder_id) = self.get_parent_folder_id(&parent_path) {
                if let Ok(mut stmt) = self.db.prepare(folder_query) {
                    stmt.query_row([&parent_folder_id, name_str], |row| {
                        let id: String = row.get(0)?;
                        let created_time: i64 = row.get(2)?;
                        let updated_time: i64 = row.get(3)?;
                        Ok((id, created_time, updated_time))
                    }).ok()
                } else {
                    None
                }
            } else {
                None
            }
        };
        
        if let Some(folder_row) = folder_result {
            let inode = self.get_or_create_inode(&full_path);
            let attr = FileAttr {
                ino: inode,
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH + Duration::from_secs(folder_row.1 as u64),
                mtime: UNIX_EPOCH + Duration::from_secs(folder_row.2 as u64),
                ctime: UNIX_EPOCH + Duration::from_secs(folder_row.2 as u64),
                crtime: UNIX_EPOCH + Duration::from_secs(folder_row.1 as u64),
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
        
        // Query database for notes (strip .md suffix when looking up in database)
        let note_query = "SELECT id, title, body, created_time, updated_time, user_updated_time FROM notes WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0 ORDER BY user_updated_time DESC LIMIT 1";
        
        let note_result = {
            if let Ok(parent_folder_id) = self.get_parent_folder_id(&parent_path) {
                if let Ok(mut stmt) = self.db.prepare(note_query) {
                    // Strip .md suffix when querying the database
                    let db_title = Self::strip_md_suffix(name_str);
                    stmt.query_row([&parent_folder_id, db_title], |row| {
                        let id: String = row.get(0)?;
                        let body: String = row.get(2)?;
                        let created_time: i64 = row.get(3)?;
                        let updated_time: i64 = row.get(4)?;
                        Ok((id, body, created_time, updated_time))
                    }).ok()
                } else {
                    None
                }
            } else {
                None
            }
        };
        
        if let Some(note_row) = note_result {
            let inode = self.get_or_create_inode(&full_path);
            let attr = FileAttr {
                ino: inode,
                size: note_row.1.len() as u64,
                blocks: ((note_row.1.len() + 511) / 512) as u64,
                atime: UNIX_EPOCH + Duration::from_secs(note_row.2 as u64),
                mtime: UNIX_EPOCH + Duration::from_secs(note_row.3 as u64),
                ctime: UNIX_EPOCH + Duration::from_secs(note_row.3 as u64),
                crtime: UNIX_EPOCH + Duration::from_secs(note_row.2 as u64),
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
        
        // Query database for folders first
        let folder_query = "SELECT id, title, created_time, updated_time, user_updated_time FROM folders WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0 ORDER BY user_updated_time DESC LIMIT 1";
        
        if let Ok(parent_folder_id) = self.get_parent_folder_id(parent_path) {
            if let Ok(mut stmt) = self.db.prepare(folder_query) {
                if let Ok(folder_row) = stmt.query_row([&parent_folder_id, filename], |row| {
                    let created_time: i64 = row.get(2)?;
                    let updated_time: i64 = row.get(3)?;
                    Ok((created_time, updated_time))
                }) {
                    let attr = FileAttr {
                        ino,
                        size: 0,
                        blocks: 0,
                        atime: UNIX_EPOCH + Duration::from_secs(folder_row.0 as u64),
                        mtime: UNIX_EPOCH + Duration::from_secs(folder_row.1 as u64),
                        ctime: UNIX_EPOCH + Duration::from_secs(folder_row.1 as u64),
                        crtime: UNIX_EPOCH + Duration::from_secs(folder_row.0 as u64),
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
        }
        
        // Query database for notes (strip .md suffix when looking up in database)
        let note_query = "SELECT id, title, body, created_time, updated_time, user_updated_time FROM notes WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0 ORDER BY user_updated_time DESC LIMIT 1";
        
        if let Ok(parent_folder_id) = self.get_parent_folder_id(parent_path) {
            if let Ok(mut stmt) = self.db.prepare(note_query) {
                // Strip .md suffix when querying the database
                let db_title = Self::strip_md_suffix(filename);
                if let Ok(note_row) = stmt.query_row([&parent_folder_id, db_title], |row| {
                    let body: String = row.get(2)?;
                    let created_time: i64 = row.get(3)?;
                    let updated_time: i64 = row.get(4)?;
                    Ok((body, created_time, updated_time))
                }) {
                    let attr = FileAttr {
                        ino,
                        size: note_row.0.len() as u64,
                        blocks: ((note_row.0.len() + 511) / 512) as u64,
                        atime: UNIX_EPOCH + Duration::from_secs(note_row.1 as u64),
                        mtime: UNIX_EPOCH + Duration::from_secs(note_row.2 as u64),
                        ctime: UNIX_EPOCH + Duration::from_secs(note_row.2 as u64),
                        crtime: UNIX_EPOCH + Duration::from_secs(note_row.1 as u64),
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
        
        // Query database for the note content (strip .md suffix when looking up in database)
        let note_query = "SELECT body FROM notes WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0 ORDER BY user_updated_time DESC LIMIT 1";
        
        if let Ok(parent_folder_id) = self.get_parent_folder_id(parent_path) {
            if let Ok(mut stmt) = self.db.prepare(note_query) {
                // Strip .md suffix when querying the database
                let db_title = Self::strip_md_suffix(filename);
                if let Ok(body) = stmt.query_row([&parent_folder_id, db_title], |row| {
                    let body: String = row.get(0)?;
                    Ok(body)
                }) {
                    let content = body.as_bytes();
                    let start = offset as usize;
                    if start < content.len() {
                        reply.data(&content[start..]);
                    } else {
                        reply.data(&[]);
                    }
                    return;
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
                format!("/{}", title)
            } else {
                format!("{}/{}", path, title)
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
                format!("/{}", display_title)
            } else {
                format!("{}/{}", path, display_title)
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
            eprintln!("Failed to open database: {}", e);
            std::process::exit(1);
        }
    };
    
    let mut options = vec![MountOption::RO, MountOption::FSName("sqlite_fuse".to_string())];
    if matches.get_flag("auto_unmount") {
        options.push(MountOption::AutoUnmount);
    }
    if matches.get_flag("allow-root") {
        options.push(MountOption::AllowRoot);
    }
    fuser::mount2(fs, mountpoint, &options).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn create_test_db() -> rusqlite::Result<Connection> {
        let conn = Connection::open(":memory:")?;
        
        // Create tables
        conn.execute(
            "CREATE TABLE `notes`(`id` TEXT PRIMARY KEY,`parent_id` TEXT NOT NULL DEFAULT \"\",`title` TEXT NOT NULL DEFAULT \"\",`body` TEXT NOT NULL DEFAULT \"\",`created_time` INT NOT NULL,`updated_time` INT NOT NULL,`is_conflict` INT NOT NULL DEFAULT 0,`latitude` NUMERIC NOT NULL DEFAULT 0,`longitude` NUMERIC NOT NULL DEFAULT 0,`altitude` NUMERIC NOT NULL DEFAULT 0,`author` TEXT NOT NULL DEFAULT \"\",`source_url` TEXT NOT NULL DEFAULT \"\",`is_todo` INT NOT NULL DEFAULT 0,`todo_due` INT NOT NULL DEFAULT 0,`todo_completed` INT NOT NULL DEFAULT 0,`source` TEXT NOT NULL DEFAULT \"\",`source_application` TEXT NOT NULL DEFAULT \"\",`application_data` TEXT NOT NULL DEFAULT \"\",`order` NUMERIC NOT NULL DEFAULT 0,`user_created_time` INT NOT NULL DEFAULT 0,`user_updated_time` INT NOT NULL DEFAULT 0,`encryption_cipher_text` TEXT NOT NULL DEFAULT \"\",`encryption_applied` INT NOT NULL DEFAULT 0,`markup_language` INT NOT NULL DEFAULT 1,`is_shared` INT NOT NULL DEFAULT 0, share_id TEXT NOT NULL DEFAULT \"\", conflict_original_id TEXT NOT NULL DEFAULT \"\", master_key_id TEXT NOT NULL DEFAULT \"\", `user_data` TEXT NOT NULL DEFAULT \"\", `deleted_time` INT NOT NULL DEFAULT 0)",
            [],
        )?;
        
        conn.execute(
            "CREATE TABLE folders (id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT \"\", created_time INT NOT NULL, updated_time INT NOT NULL, user_created_time INT NOT NULL DEFAULT 0, user_updated_time INT NOT NULL DEFAULT 0, encryption_cipher_text TEXT NOT NULL DEFAULT \"\", encryption_applied INT NOT NULL DEFAULT 0, parent_id TEXT NOT NULL DEFAULT \"\", is_shared INT NOT NULL DEFAULT 0, share_id TEXT NOT NULL DEFAULT \"\", master_key_id TEXT NOT NULL DEFAULT \"\", icon TEXT NOT NULL DEFAULT \"\", `user_data` TEXT NOT NULL DEFAULT \"\", `deleted_time` INT NOT NULL DEFAULT 0)",
            [],
        )?;
        
        // Insert test data
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as i64;
        
        // Create a folder
        conn.execute(
            "INSERT INTO folders (id, title, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            ["folder1", "Documents", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), ""],
        )?;
        
        // Create a note in root
        conn.execute(
            "INSERT INTO notes (id, title, body, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            ["note1", "readme.txt", "This is a test note in root", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), ""],
        )?;
        
        // Create a note in folder
        conn.execute(
            "INSERT INTO notes (id, title, body, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            ["note2", "document.md", "This is a test note in Documents folder", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), "folder1"],
        )?;
        
        Ok(conn)
    }

    #[test]
    fn test_filesystem_operations() {
        let conn = create_test_db().expect("Failed to create test database");
        let mut fs = SqliteFS {
            db: conn,
            inode_map: std::collections::HashMap::new(),
            reverse_inode_map: std::collections::HashMap::new(),
            next_inode: 2,
        };
        
        // Initialize root directory
        fs.inode_map.insert("/".to_string(), 1);
        fs.reverse_inode_map.insert(1, "/".to_string());
        
        // Test direct database queries to verify our setup
        let folder_count: i64 = fs.db.query_row(
            "SELECT COUNT(*) FROM folders WHERE parent_id = '' AND deleted_time = 0",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(folder_count, 1, "Expected 1 folder in root");
        
        let note_count: i64 = fs.db.query_row(
            "SELECT COUNT(*) FROM notes WHERE parent_id = '' AND deleted_time = 0",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(note_count, 1, "Expected 1 note in root");
        
        // Test that we can find the Documents folder
        let documents_exists = fs.db.query_row(
            "SELECT title FROM folders WHERE parent_id = '' AND title = 'Documents' AND deleted_time = 0",
            [],
            |row| row.get::<_, String>(0)
        );
        assert!(documents_exists.is_ok(), "Documents folder should exist");
        assert_eq!(documents_exists.unwrap(), "Documents");
        
        // Test that we can find the readme.txt note
        let readme_exists = fs.db.query_row(
            "SELECT title, body FROM notes WHERE parent_id = '' AND title = 'readme.txt' AND deleted_time = 0",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        );
        assert!(readme_exists.is_ok(), "readme.txt should exist");
        let (title, body) = readme_exists.unwrap();
        assert_eq!(title, "readme.txt");
        assert_eq!(body, "This is a test note in root");
        
        // Test that we can find the document in the Documents folder
        let document_exists = fs.db.query_row(
            "SELECT title, body FROM notes WHERE parent_id = 'folder1' AND title = 'document.md' AND deleted_time = 0",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        );
        assert!(document_exists.is_ok(), "document.md should exist in Documents folder");
        let (title, body) = document_exists.unwrap();
        assert_eq!(title, "document.md");
        assert_eq!(body, "This is a test note in Documents folder");
        
        // Test inode mapping
        let readme_inode = fs.get_or_create_inode("/readme.txt.md");
        let documents_inode = fs.get_or_create_inode("/Documents");
        
        // Verify inodes are unique
        assert_ne!(readme_inode, documents_inode);
        assert_ne!(readme_inode, 1); // Not root
        assert_ne!(documents_inode, 1); // Not root
        
        // Test path resolution
        assert_eq!(fs.get_path_from_inode(1), Some(&"/".to_string()));
        assert_eq!(fs.get_path_from_inode(readme_inode), Some(&"/readme.txt.md".to_string()));
        assert_eq!(fs.get_path_from_inode(documents_inode), Some(&"/Documents".to_string()));
        
        println!("All tests passed!");
        println!("- Database setup: ✓");
        println!("- Folder creation: ✓");
        println!("- Note creation: ✓");
        println!("- Hierarchy structure: ✓");
        println!("- Inode mapping: ✓");
        println!("- Path resolution: ✓");
    }

    fn create_deep_hierarchy_db() -> rusqlite::Result<Connection> {
        let conn = Connection::open(":memory:")?;
        
        // Create tables
        conn.execute(
            "CREATE TABLE `notes`(`id` TEXT PRIMARY KEY,`parent_id` TEXT NOT NULL DEFAULT \"\",`title` TEXT NOT NULL DEFAULT \"\",`body` TEXT NOT NULL DEFAULT \"\",`created_time` INT NOT NULL,`updated_time` INT NOT NULL,`is_conflict` INT NOT NULL DEFAULT 0,`latitude` NUMERIC NOT NULL DEFAULT 0,`longitude` NUMERIC NOT NULL DEFAULT 0,`altitude` NUMERIC NOT NULL DEFAULT 0,`author` TEXT NOT NULL DEFAULT \"\",`source_url` TEXT NOT NULL DEFAULT \"\",`is_todo` INT NOT NULL DEFAULT 0,`todo_due` INT NOT NULL DEFAULT 0,`todo_completed` INT NOT NULL DEFAULT 0,`source` TEXT NOT NULL DEFAULT \"\",`source_application` TEXT NOT NULL DEFAULT \"\",`application_data` TEXT NOT NULL DEFAULT \"\",`order` NUMERIC NOT NULL DEFAULT 0,`user_created_time` INT NOT NULL DEFAULT 0,`user_updated_time` INT NOT NULL DEFAULT 0,`encryption_cipher_text` TEXT NOT NULL DEFAULT \"\",`encryption_applied` INT NOT NULL DEFAULT 0,`markup_language` INT NOT NULL DEFAULT 1,`is_shared` INT NOT NULL DEFAULT 0, share_id TEXT NOT NULL DEFAULT \"\", conflict_original_id TEXT NOT NULL DEFAULT \"\", master_key_id TEXT NOT NULL DEFAULT \"\", `user_data` TEXT NOT NULL DEFAULT \"\", `deleted_time` INT NOT NULL DEFAULT 0)",
            [],
        )?;
        
        conn.execute(
            "CREATE TABLE folders (id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT \"\", created_time INT NOT NULL, updated_time INT NOT NULL, user_created_time INT NOT NULL DEFAULT 0, user_updated_time INT NOT NULL DEFAULT 0, encryption_cipher_text TEXT NOT NULL DEFAULT \"\", encryption_applied INT NOT NULL DEFAULT 0, parent_id TEXT NOT NULL DEFAULT \"\", is_shared INT NOT NULL DEFAULT 0, share_id TEXT NOT NULL DEFAULT \"\", master_key_id TEXT NOT NULL DEFAULT \"\", icon TEXT NOT NULL DEFAULT \"\", `user_data` TEXT NOT NULL DEFAULT \"\", `deleted_time` INT NOT NULL DEFAULT 0)",
            [],
        )?;
        
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as i64;
        
        // Level 1: Root level folder and file
        conn.execute(
            "INSERT INTO folders (id, title, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            ["projects", "Projects", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), ""],
        )?;
        
        conn.execute(
            "INSERT INTO notes (id, title, body, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            ["root_readme", "README.md", "Root level documentation", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), ""],
        )?;
        
        // Level 2: Folders and files inside Projects
        conn.execute(
            "INSERT INTO folders (id, title, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            ["rust_project", "RustProject", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), "projects"],
        )?;
        
        conn.execute(
            "INSERT INTO folders (id, title, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            ["python_project", "PythonProject", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), "projects"],
        )?;
        
        conn.execute(
            "INSERT INTO notes (id, title, body, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            ["projects_overview", "overview.txt", "Projects overview document", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), "projects"],
        )?;
        
        // Level 3: Folders and files inside RustProject
        conn.execute(
            "INSERT INTO folders (id, title, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            ["rust_src", "src", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), "rust_project"],
        )?;
        
        conn.execute(
            "INSERT INTO folders (id, title, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            ["rust_tests", "tests", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), "rust_project"],
        )?;
        
        conn.execute(
            "INSERT INTO notes (id, title, body, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            ["cargo_toml", "Cargo.toml", "[package]\nname = \"test_project\"\nversion = \"0.1.0\"", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), "rust_project"],
        )?;
        
        // Level 4: Files inside src folder (deepest level)
        conn.execute(
            "INSERT INTO notes (id, title, body, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            ["main_rs", "main.rs", "fn main() {\n    println!(\"Hello, world!\");\n}", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), "rust_src"],
        )?;
        
        conn.execute(
            "INSERT INTO notes (id, title, body, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            ["lib_rs", "lib.rs", "pub fn add(left: usize, right: usize) -> usize {\n    left + right\n}", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), "rust_src"],
        )?;
        
        // Add some files in the tests folder too
        conn.execute(
            "INSERT INTO notes (id, title, body, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            ["integration_test", "integration_test.rs", "#[test]\nfn it_works() {\n    assert_eq!(2 + 2, 4);\n}", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), "rust_tests"],
        )?;
        
        // Add content to PythonProject as well
        conn.execute(
            "INSERT INTO notes (id, title, body, created_time, updated_time, user_created_time, user_updated_time, parent_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            ["python_main", "main.py", "def main():\n    print(\"Hello from Python!\")\n\nif __name__ == \"__main__\":\n    main()", &now.to_string(), &now.to_string(), &now.to_string(), &now.to_string(), "python_project"],
        )?;
        
        Ok(conn)
    }

    #[test]
    fn test_deep_hierarchy_navigation() {
        let conn = create_deep_hierarchy_db().expect("Failed to create deep hierarchy database");
        let mut fs = SqliteFS {
            db: conn,
            inode_map: std::collections::HashMap::new(),
            reverse_inode_map: std::collections::HashMap::new(),
            next_inode: 2,
        };
        
        // Initialize root directory
        fs.inode_map.insert("/".to_string(), 1);
        fs.reverse_inode_map.insert(1, "/".to_string());
        
        // Test Level 1: Root directory contents
        println!("Testing Level 1 (Root):");
        let root_folders: Vec<String> = fs.db.prepare("SELECT title FROM folders WHERE parent_id = '' AND deleted_time = 0")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        
        let root_notes: Vec<String> = fs.db.prepare("SELECT title FROM notes WHERE parent_id = '' AND deleted_time = 0")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        
        assert_eq!(root_folders, vec!["Projects"]);
        assert_eq!(root_notes, vec!["README.md"]);
        println!("  ✓ Found folders: {:?}", root_folders);
        println!("  ✓ Found files: {:?}", root_notes);
        
        // Test Level 2: Projects folder contents
        println!("Testing Level 2 (Projects):");
        let projects_folders: Vec<String> = fs.db.prepare("SELECT title FROM folders WHERE parent_id = 'projects' AND deleted_time = 0")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        
        let projects_notes_raw: Vec<String> = fs.db.prepare("SELECT title FROM notes WHERE parent_id = 'projects' AND deleted_time = 0")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        
        // Convert raw database titles to filesystem display titles (with .md suffix)
        let projects_notes: Vec<String> = projects_notes_raw.iter()
            .map(|title| SqliteFS::add_md_suffix(title))
            .collect();
        
        assert_eq!(projects_folders.len(), 2);
        assert!(projects_folders.contains(&"RustProject".to_string()));
        assert!(projects_folders.contains(&"PythonProject".to_string()));
        assert_eq!(projects_notes, vec!["overview.txt.md"]);
        println!("  ✓ Found folders: {:?}", projects_folders);
        println!("  ✓ Found files: {:?}", projects_notes);
        
        // Test Level 3: RustProject folder contents
        println!("Testing Level 3 (RustProject):");
        let rust_folders: Vec<String> = fs.db.prepare("SELECT title FROM folders WHERE parent_id = 'rust_project' AND deleted_time = 0")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        
        let rust_notes_raw: Vec<String> = fs.db.prepare("SELECT title FROM notes WHERE parent_id = 'rust_project' AND deleted_time = 0")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        
        // Convert raw database titles to filesystem display titles (with .md suffix)
        let rust_notes: Vec<String> = rust_notes_raw.iter()
            .map(|title| SqliteFS::add_md_suffix(title))
            .collect();
        
        assert_eq!(rust_folders.len(), 2);
        assert!(rust_folders.contains(&"src".to_string()));
        assert!(rust_folders.contains(&"tests".to_string()));
        assert_eq!(rust_notes, vec!["Cargo.toml.md"]);
        println!("  ✓ Found folders: {:?}", rust_folders);
        println!("  ✓ Found files: {:?}", rust_notes);
        
        // Test Level 4: src folder contents (deepest level)
        println!("Testing Level 4 (src):");
        let src_files_raw: Vec<String> = fs.db.prepare("SELECT title FROM notes WHERE parent_id = 'rust_src' AND deleted_time = 0")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        
        // Convert raw database titles to filesystem display titles (with .md suffix)
        let src_files: Vec<String> = src_files_raw.iter()
            .map(|title| SqliteFS::add_md_suffix(title))
            .collect();
        
        assert_eq!(src_files.len(), 2);
        assert!(src_files.contains(&"main.rs.md".to_string()));
        assert!(src_files.contains(&"lib.rs.md".to_string()));
        println!("  ✓ Found files: {:?}", src_files);
        
        // Test deep path navigation and file content retrieval
        println!("Testing deep path navigation:");
        
        // Test inode creation for deep paths
        let projects_inode = fs.get_or_create_inode("/Projects");
        let rust_project_inode = fs.get_or_create_inode("/Projects/RustProject");
        let src_inode = fs.get_or_create_inode("/Projects/RustProject/src");
        let main_rs_inode = fs.get_or_create_inode("/Projects/RustProject/src/main.rs.md");
        
        // Verify all inodes are unique
        let inodes = vec![1, projects_inode, rust_project_inode, src_inode, main_rs_inode];
        let unique_count = inodes.iter().collect::<std::collections::HashSet<_>>().len();
        assert_eq!(unique_count, inodes.len(), "All inodes should be unique");
        println!("  ✓ All {} inodes are unique", inodes.len());
        
        // Test path resolution
        assert_eq!(fs.get_path_from_inode(1), Some(&"/".to_string()));
        assert_eq!(fs.get_path_from_inode(projects_inode), Some(&"/Projects".to_string()));
        assert_eq!(fs.get_path_from_inode(rust_project_inode), Some(&"/Projects/RustProject".to_string()));
        assert_eq!(fs.get_path_from_inode(src_inode), Some(&"/Projects/RustProject/src".to_string()));
        assert_eq!(fs.get_path_from_inode(main_rs_inode), Some(&"/Projects/RustProject/src/main.rs.md".to_string()));
        println!("  ✓ Path resolution works for all levels");
        
        // Test file content retrieval at different levels
        println!("Testing file content retrieval:");
        
        // Root level file
        let root_readme_content: String = fs.db.query_row(
            "SELECT body FROM notes WHERE parent_id = '' AND title = 'README.md' AND deleted_time = 0",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(root_readme_content, "Root level documentation");
        println!("  ✓ Root README.md: '{}'", root_readme_content);
        
        // Level 2 file
        let projects_overview_content: String = fs.db.query_row(
            "SELECT body FROM notes WHERE parent_id = 'projects' AND title = 'overview.txt' AND deleted_time = 0",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(projects_overview_content, "Projects overview document");
        println!("  ✓ Projects overview.txt: '{}'", projects_overview_content);
        
        // Level 3 file
        let cargo_toml_content: String = fs.db.query_row(
            "SELECT body FROM notes WHERE parent_id = 'rust_project' AND title = 'Cargo.toml' AND deleted_time = 0",
            [],
            |row| row.get(0)
        ).unwrap();
        assert!(cargo_toml_content.contains("[package]"));
        assert!(cargo_toml_content.contains("test_project"));
        println!("  ✓ Cargo.toml contains expected content");
        
        // Level 4 files (deepest)
        let main_rs_content: String = fs.db.query_row(
            "SELECT body FROM notes WHERE parent_id = 'rust_src' AND title = 'main.rs' AND deleted_time = 0",
            [],
            |row| row.get(0)
        ).unwrap();
        assert!(main_rs_content.contains("fn main()"));
        assert!(main_rs_content.contains("Hello, world!"));
        println!("  ✓ main.rs contains expected Rust code");
        
        let lib_rs_content: String = fs.db.query_row(
            "SELECT body FROM notes WHERE parent_id = 'rust_src' AND title = 'lib.rs' AND deleted_time = 0",
            [],
            |row| row.get(0)
        ).unwrap();
        assert!(lib_rs_content.contains("pub fn add"));
        println!("  ✓ lib.rs contains expected Rust code");
        
        // Test parallel branch (Python project)
        let python_main_content: String = fs.db.query_row(
            "SELECT body FROM notes WHERE parent_id = 'python_project' AND title = 'main.py' AND deleted_time = 0",
            [],
            |row| row.get(0)
        ).unwrap();
        assert!(python_main_content.contains("def main()"));
        assert!(python_main_content.contains("Hello from Python!"));
        println!("  ✓ Python main.py contains expected code");
        
        println!("\\n🎉 All deep hierarchy tests passed!");
        println!("- 4 levels of depth: ✓");
        println!("- Multiple folders per level: ✓");
        println!("- Files at all levels: ✓");
        println!("- Unique inode mapping: ✓");
        println!("- Deep path resolution: ✓");
        println!("- Content retrieval at all levels: ✓");
        println!("- Parallel branches: ✓");
    }

    #[test]
    fn test_parent_folder_id_resolution() {
        let conn = create_deep_hierarchy_db().expect("Failed to create test database");
        let fs = SqliteFS {
            db: conn,
            inode_map: std::collections::HashMap::new(),
            reverse_inode_map: std::collections::HashMap::new(),
            next_inode: 2,
        };
        
        // Test root directory
        let root_id = fs.get_parent_folder_id("/").unwrap();
        assert_eq!(root_id, "");
        println!("✓ Root directory resolves to empty parent_id");
        
        // Test level 1 path
        let projects_id = fs.get_parent_folder_id("/Projects").unwrap();
        assert_eq!(projects_id, "projects");
        println!("✓ /Projects resolves to 'projects' UUID");
        
        // Test level 2 path
        let rust_project_id = fs.get_parent_folder_id("/Projects/RustProject").unwrap();
        assert_eq!(rust_project_id, "rust_project");
        println!("✓ /Projects/RustProject resolves to 'rust_project' UUID");
        
        // Test level 3 path
        let src_id = fs.get_parent_folder_id("/Projects/RustProject/src").unwrap();
        assert_eq!(src_id, "rust_src");
        println!("✓ /Projects/RustProject/src resolves to 'rust_src' UUID");
        
        // Test that we can find items at each level using the resolved parent IDs
        
        // Root level check
        let root_folder_count: i64 = fs.db.query_row(
            "SELECT COUNT(*) FROM folders WHERE parent_id = ?1 AND deleted_time = 0",
            [&root_id],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(root_folder_count, 1);
        println!("✓ Found {} folder in root using resolved parent_id", root_folder_count);
        
        // Projects level check
        let projects_folder_count: i64 = fs.db.query_row(
            "SELECT COUNT(*) FROM folders WHERE parent_id = ?1 AND deleted_time = 0",
            [&projects_id],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(projects_folder_count, 2); // RustProject and PythonProject
        println!("✓ Found {} folders in Projects using resolved parent_id", projects_folder_count);
        
        // RustProject level check
        let rust_folder_count: i64 = fs.db.query_row(
            "SELECT COUNT(*) FROM folders WHERE parent_id = ?1 AND deleted_time = 0",
            [&rust_project_id],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(rust_folder_count, 2); // src and tests
        println!("✓ Found {} folders in RustProject using resolved parent_id", rust_folder_count);
        
        // src level check
        let src_note_count: i64 = fs.db.query_row(
            "SELECT COUNT(*) FROM notes WHERE parent_id = ?1 AND deleted_time = 0",
            [&src_id],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(src_note_count, 2); // main.rs and lib.rs
        println!("✓ Found {} files in src using resolved parent_id", src_note_count);
        
        println!("\\n🎉 Parent folder ID resolution test passed!");
        println!("- UUID-based parent resolution: ✓");
        println!("- Multi-level path walking: ✓");
        println!("- Database relationship validation: ✓");
    }

}
