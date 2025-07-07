# SQLite FUSE Filesystem

A FUSE (Filesystem in Userspace) implementation that presents SQLite Joplin database as a filesystem by mapping the `title` field from the `notes` and `folders` tables to a file path.

## Usage

```
git clone https://github.com/RyanGreenup/joplin_sqlite_fuse
cd joplin_sqlite_fuse
cargo run --release -- ~/.config/joplin-desktop/database.sqlite $dir
```

Across machines this can be combined with `sshfs`:

```
sshfs remote:/tmp/mount /tmp/mount -o auto_cache,reconnect
```



## Features

### Core Functionality

- **File Operations**: Create, read, edit, rename, and delete files
- **Directory Operations**: Create, list, rename, and delete directories
- **Move Operations**: Move files and folders between directories
- **Editor Support**: Compatible with text editors and tooling such as `yazi`, `ranger` and `broot`.
- **Markdown Files**: All files appear with `.md` extension in the filesystem

### SQL Notes

- **Database Indexes**: Automatically creates required indexes (e.g. `idx_folders_parent_title`) to improve performance. See [The source](src/main.rs)
- **Conflict Resolution**: Handles entries with identical paths paths duplicate entries by favoring most recent `user_updated_time`

## Database Schema

The database schema is consistent with Joplin and could be adapted later.


### Notes Table (Files)
```sql
CREATE TABLE notes (
    id TEXT PRIMARY KEY,                -- UUID v4
    parent_id TEXT NOT NULL DEFAULT "", -- References folders.id
    title TEXT NOT NULL DEFAULT "",     -- Filename (without .md extension)
    body TEXT NOT NULL DEFAULT "",      -- File content
    created_time INT NOT NULL,
    updated_time INT NOT NULL,
    user_updated_time INT NOT NULL,
    deleted_time INT NOT NULL DEFAULT 0,
    -- Additional fields for application metadata...
);
```

### Folders Table (Directories)
```sql
CREATE TABLE folders (
    id TEXT PRIMARY KEY,                -- UUID v4
    parent_id TEXT NOT NULL DEFAULT "", -- References folders.id (empty for root)
    title TEXT NOT NULL DEFAULT "",     -- Directory name
    created_time INT NOT NULL,
    updated_time INT NOT NULL,
    user_updated_time INT NOT NULL,
    deleted_time INT NOT NULL DEFAULT 0,
    -- Additional fields for application metadata...
);
```

## Installation & Usage

### Prerequisites
- Rust (latest stable version)
- FUSE development libraries
  ```bash
  # Ubuntu/Debian
  sudo apt install libfuse-dev

  # macOS
  brew install macfuse
  ```

### Building
```bash
git clone <repository-url>
cd sqlite_fuse
cargo build --release
```

### Running
```bash
# Create mount point
mkdir /tmp/my_mount

# Mount filesystem
cargo run --release -- database.sqlite /tmp/my_mount
# when finished:
# umount /tmp/my_mount

# Or using the binary
./target/release/sqlite_fuse database.sqlite /tmp/my_mount
```

### Usage Examples
```bash
# List files and directories
ls /tmp/my_mount

# Create a new directory
mkdir /tmp/my_mount/projects

# Create a new file
echo "# My Note" > /tmp/my_mount/projects/note.md

# Edit files with any editor
vim /tmp/my_mount/projects/note.md
emacs /tmp/my_mount/projects/note.md

# Move files between directories
mv /tmp/my_mount/projects/note.md /tmp/my_mount/archive/

# Rename files and directories
mv /tmp/my_mount/projects /tmp/my_mount/work

# Delete files and directories
rm /tmp/my_mount/archive/note.md
rmdir /tmp/my_mount/empty_dir

# Unmount when done
fusermount -u /tmp/my_mount
```

## Command Line Options

```bash
sqlite_fuse [OPTIONS] <DATABASE> <MOUNT_POINT>

Arguments:
  <DATABASE>     Path to the SQLite database file
  <MOUNT_POINT>  Directory where the filesystem will be mounted

Options:
  --auto_unmount    Automatically unmount on process exit
  --allow-root      Allow root user to access filesystem
  -h, --help        Print help information
```

## Architecture

### FUSE Operations
The filesystem implements all standard FUSE operations:
- `lookup()`: File/directory name resolution
- `getattr()`: File attribute retrieval
- `read()`: File content reading
- `readdir()`: Directory listing
- `write()`: File content modification
- `create()`: File creation
- `mkdir()`: Directory creation
- `rename()`: File/directory renaming and moving
- `unlink()`: File deletion
- `rmdir()`: Directory deletion
- `setattr()`, `flush()`, `release()`: Editor compatibility

### Key Design Decisions

1. **UUID-based IDs**: Whilst the notes have `UUID` the `title` field is used, otherwise there would be limited benefit to exposing the database as a filesystem.
    - In the future, the `id` may be included as a YAML header
3. **Markdown Extension**: Files automatically display with `.md` suffix but store title without extension
    - In the future, the extension may be dynamic (e.g. `.html`, `.md`, `.org`)
4. **Conflict Resolution**: When multiple entries have the same path, the most recent `user_updated_time` wins
5. **Performance Indexes**: Automatic creation of database indexes on mount for optimal query performance
    - Without these indexes `find` takes a minute, after applying the indexes it takes 5 seconds.


### Dependencies
- `fuser`: FUSE filesystem framework
- `rusqlite`: SQLite database interface
- `uuid`: UUID generation
- `clap`: Command-line argument parsing
- `env_logger`: Logging functionality


## Troubleshooting

### Common Issues

1. **Permission Denied**: Ensure FUSE permissions and mount point accessibility
2. **Database Lock**: Close other applications accessing the SQLite database
    - Or set journal mode to `WAL`
3. **Mount Fails**: Check if mount point exists and is empty
    ```sh
    mkdir  -p ${dir_path}
    umount -l ${dir_path}
    ```
4. **Performance Issues**: Ensure database indexes are created (automatic on first mount)

### Debugging
Enable debug logging to see detailed operation traces:
```bash
RUST_LOG=debug cargo run -- database.sqlite /tmp/mount
```

### Unmounting
If the filesystem becomes unresponsive:

```bash
# Force unmount
fusermount -u /tmp/mount

# Or if that fails
sudo umount -f /tmp/mount
```

## License

GPL

