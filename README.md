# google-fs-recover
Google Filestream Forensic Tool

About: This tool is intended to scan the Google File Stream sqllitedb and match up original files names with files in the Google File Stream cache. It will also produce a MD5 hash of the file with an option to copy the files to a new location with the original file names.

This is a very raw tool. Use at your own risk! 

Example Usage:
python .\google-fs-recover.py -db <metadata_sqlite_db full path> -c <content_cache full path> -csv <outputfile full path> -b <filebackup full path>