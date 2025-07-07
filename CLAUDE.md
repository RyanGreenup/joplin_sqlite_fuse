# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Rust project that implements a FUSE (Filesystem in Userspace) filesystem using the `fuser` crate for a sqlite database with the following schema:

```sql
-- Notes correspond to Files
CREATE TABLE `notes`(`id` TEXT PRIMARY KEY,`parent_id` TEXT NOT NULL DEFAULT "",`title` TEXT NOT NULL DEFAULT "",`body` TEXT NOT NULL DEFAULT "",`created_time` INT NOT NULL,`updated_time` INT NOT NULL,`is_conflict` INT NOT NULL DEFAULT 0,`latitude` NUMERIC NOT NULL DEFAULT 0,`longitude` NUMERIC NOT NULL DEFAULT 0,`altitude` NUMERIC NOT NULL DEFAULT 0,`author` TEXT NOT NULL DEFAULT "",`source_url` TEXT NOT NULL DEFAULT "",`is_todo` INT NOT NULL DEFAULT 0,`todo_due` INT NOT NULL DEFAULT 0,`todo_completed` INT NOT NULL DEFAULT 0,`source` TEXT NOT NULL DEFAULT "",`source_application` TEXT NOT NULL DEFAULT "",`application_data` TEXT NOT NULL DEFAULT "",`order` NUMERIC NOT NULL DEFAULT 0,`user_created_time` INT NOT NULL DEFAULT 0,`user_updated_time` INT NOT NULL DEFAULT 0,`encryption_cipher_text` TEXT NOT NULL DEFAULT "",`encryption_applied` INT NOT NULL DEFAULT 0,`markup_language` INT NOT NULL DEFAULT 1,`is_shared` INT NOT NULL DEFAULT 0, share_id TEXT NOT NULL DEFAULT "", conflict_original_id TEXT NOT NULL DEFAULT "", master_key_id TEXT NOT NULL DEFAULT "", `user_data` TEXT NOT NULL DEFAULT "", `deleted_time` INT NOT NULL DEFAULT 0);

-- Folders correspond to Files
CREATE TABLE folders (id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT "", created_time INT NOT NULL, updated_time INT NOT NULL, user_created_time INT NOT NULL DEFAULT 0, user_updated_time INT NOT NULL DEFAULT 0, encryption_cipher_text TEXT NOT NULL DEFAULT "", encryption_applied INT NOT NULL DEFAULT 0, parent_id TEXT NOT NULL DEFAULT "", is_shared INT NOT NULL DEFAULT 0, share_id TEXT NOT NULL DEFAULT "", master_key_id TEXT NOT NULL DEFAULT "", icon TEXT NOT NULL DEFAULT "", `user_data` TEXT NOT NULL DEFAULT "", `deleted_time` INT NOT NULL DEFAULT 0);
```

In this context the `notes` table are files and the `folders` table is folders. The `title` field should be used as the file / directory name. parent_id refers to a folder. Where there is a conflict because two rows have the same path based on title, always favour the most resent based on user_updated_time.

The initial starting implementation was / is a simple "Hello World" filesystem that presents a single file `hello.txt` containing "Hello World!\n".

For testing purposes the ./database.sqlite file may be used.

The user cares primarily with:

1. Creating
    - [ ] Folders
    - [ ] Files
2. Read
    - [X] Listing Content under Directories
    - [X] Reading the contents of a file (e.g. `cat`)
3. Update
    - [ ] Renaming Files (to change the `title`)
    - [ ] Renaming Folders (to change the `title`)
    - [ ] Changing the content of a note (to change the `body`)
    - [ ] Moving Folders and all children recursively (e.g. `mv`)
    - [ ] Moving Files
4. Delete
    - [ ] Deleting Files (`rm`)
    - [ ] Deleting Folders (`rm -r`)

## Development Commands

- **Build the project**: `cargo build`
- **Run the project**: `mkdir -p /tmp/testing_dir; cargo run -- /tmp/testing_dir` (requires a mount point directory)
- **Check code without building**: `cargo check`
- **Run with logging**: `RUST_LOG=debug cargo run -- <mount_point>`

## Architecture

The project follows a simple FUSE filesystem architecture:

- **HelloFS struct**: Implements the `Filesystem` trait from the `fuser` crate
- **Core FUSE operations implemented**:
  - `lookup()`: File/directory name resolution
  - `getattr()`: File attribute retrieval
  - `read()`: File content reading
  - `readdir()`: Directory listing
- **Hardcoded filesystem structure**: Root directory (inode 1) contains a single file `hello.txt` (inode 2)
- **Static file attributes**: Uses constant `FileAttr` structs for consistent metadata

## Documentation

  * https://docs.rs/fuser/latest/fuser/trait.Filesystem.html

## Key Dependencies

- `fuser`: FUSE filesystem implementation
- `clap`: Command-line argument parsing
- `env_logger`: Logging functionality
- `libc`: System call constants

## Testing the Filesystem

To test the filesystem:
1. Create a mount point directory: `mkdir /tmp/test_mount`
2. Run the filesystem: `cargo run -- /tmp/test_mount`
3. In another terminal, test operations: `ls /tmp/test_mount` and `cat /tmp/test_mount/hello.txt`
4. Unmount when done: `fusermount -u /tmp/test_mount`

## Project Structure

- `src/main.rs`: Contains the complete FUSE filesystem implementation
- `Cargo.toml`: Project dependencies and metadata
- `database.sqlite`: SQLite database file (appears unused in current implementation)
