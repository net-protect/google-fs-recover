# google-fs-recover
Google Filestream Forensic Tool

About: This tool is intended to scan the Google File Stream sqllitedb and match up original files names with files in the Google File Stream cache. It will also produce a MD5 hash of the file with an option to copy the files to a new location with the original file names.

Use at your own risk! 

Example Usage:
python .\google-fs-recover.py -p <googlefs_path> -csv <csv_output_path> -b <backup_folder_path>

Contributors:
Chad Tilbury (@chadtilbury) - For providing the research and ideas to make this tool possible