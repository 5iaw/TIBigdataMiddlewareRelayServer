import os


def delete_local_file(local_file_path: str) -> None:
    try:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
    except Exception as e:
        print(f"Failed to delete {local_file_path}: {str(e)}")


def save_uploaded_file(file, upload_folder: str) -> str:
    filename = file.filename
    local_file_path = os.path.join(upload_folder, filename)
    file.save(local_file_path)
    return local_file_path

