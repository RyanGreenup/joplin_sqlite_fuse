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
            if let Ok(mut stmt) = self.db.prepare(folder_query) {
                let parent_id = if parent == 1 { "" } else { &parent_path[1..] }; // Remove leading slash for parent_id
                stmt.query_row([parent_id, name_str], |row| {
                    let id: String = row.get(0)?;
                    let created_time: i64 = row.get(2)?;
                    let updated_time: i64 = row.get(3)?;
                    Ok((id, created_time, updated_time))
                }).ok()
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
        
        // Query database for notes
        let note_query = "SELECT id, title, body, created_time, updated_time, user_updated_time FROM notes WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0 ORDER BY user_updated_time DESC LIMIT 1";
        
        let note_result = {
            if let Ok(mut stmt) = self.db.prepare(note_query) {
                let parent_id = if parent == 1 { "" } else { &parent_path[1..] }; // Remove leading slash for parent_id
                stmt.query_row([parent_id, name_str], |row| {
                    let id: String = row.get(0)?;
                    let body: String = row.get(2)?;
                    let created_time: i64 = row.get(3)?;
                    let updated_time: i64 = row.get(4)?;
                    Ok((id, body, created_time, updated_time))
                }).ok()
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
        
        if let Ok(mut stmt) = self.db.prepare(folder_query) {
            let parent_id = if parent_path == "/" { "" } else { &parent_path[1..] };
            if let Ok(folder_row) = stmt.query_row([parent_id, filename], |row| {
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
        
        // Query database for notes
        let note_query = "SELECT id, title, body, created_time, updated_time, user_updated_time FROM notes WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0 ORDER BY user_updated_time DESC LIMIT 1";
        
        if let Ok(mut stmt) = self.db.prepare(note_query) {
            let parent_id = if parent_path == "/" { "" } else { &parent_path[1..] };
            if let Ok(note_row) = stmt.query_row([parent_id, filename], |row| {
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
        
        // Query database for the note content
        let note_query = "SELECT body FROM notes WHERE parent_id = ?1 AND title = ?2 AND deleted_time = 0 ORDER BY user_updated_time DESC LIMIT 1";
        
        if let Ok(mut stmt) = self.db.prepare(note_query) {
            let parent_id = if parent_path == "/" { "" } else { &parent_path[1..] };
            if let Ok(body) = stmt.query_row([parent_id, filename], |row| {
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
        
        let parent_id = if path == "/" { "" } else { &path[1..] };
        
        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (1, FileType::Directory, "..".to_string()),
        ];
        
        // Query folders
        let folder_query = "SELECT id, title FROM folders WHERE parent_id = ?1 AND deleted_time = 0 ORDER BY user_updated_time DESC";
        let folder_titles = {
            if let Ok(mut stmt) = self.db.prepare(folder_query) {
                if let Ok(rows) = stmt.query_map([parent_id], |row| {
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
                if let Ok(rows) = stmt.query_map([parent_id], |row| {
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
            let full_path = if path == "/" {
                format!("/{}", title)
            } else {
                format!("{}/{}", path, title)
            };
            let inode = self.get_or_create_inode(&full_path);
            entries.push((inode, FileType::RegularFile, title));
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
