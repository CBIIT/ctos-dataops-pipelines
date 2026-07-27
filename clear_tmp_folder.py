import shutil
from pathlib import Path

def clear_tmp_folder(data_path = "/tmp"):
    # Define the target directory path
    tmp_path = Path(data_path)
    
    # Ensure the path exists and is a directory before proceeding
    if not tmp_path.exists() or not tmp_path.is_dir():
        print("The /tmp directory does not exist.")
        return

    # Loop through every file and subfolder inside /tmp
    for item in tmp_path.iterdir():
        try:
            if item.is_file() or item.is_symlink():
                item.unlink()  # Deletes files or symbolic links
                print(f"Deleted file: {item}")
            elif item.is_dir():
                shutil.rmtree(item)  # Deletes folder and all its contents
                print(f"Deleted folder: {item}")
        except PermissionError:
            # Common in /tmp as other users or root may own some files
            print(f"Permission denied: Could not delete {item}")
        except Exception as e:
            print(f"Failed to delete {item}. Reason: {e}")

def show_tmp_folder(data_path = "/tmp"):
    tmp_path = Path(data_path)
    if not tmp_path.exists() or not tmp_path.is_dir():
        print(f"The {data_path} directory does not exist.")
        return 0
    for item in tmp_path.iterdir():
        print(item)
    return len(list(tmp_path.iterdir()))
