import os
import sqlite3
import sys
import argparse
import csv
import shutil
import hashlib

def GetArgs(_args):#Get cmdline opts
	allArgs = argparse.ArgumentParser()
	
	allArgs.add_argument("-db","--db", required=True, help="Source of GoogleFS metadata_sqlite_db")
	allArgs.add_argument("-c","--cache", required=True, help="Source of GoogleFS Cached Files")
	allArgs.add_argument("-csv","--csv", required=True, help="Destination of CSV output")
	allArgs.add_argument("-b","--backup", required=False, help="Copy files to new location with new file names. Output path.")
	
	return vars(allArgs.parse_args())

def GetSqlData(_path):
	#TODO - Need Error Handling. 
	#Variables
	selectStr = "SELECT stable_id, local_title from items where is_folder = 0"
	files = {}
	if not (os.path.exists(_path)): #Check if sqllite db exists
		raise Exception(f"SQLlite database does not exist at {_path}")
	#sqllite logic
	conn = sqlite3.connect(_path)
	cur = conn.cursor()
	for data in cur.execute(selectStr):
		files[data[0]] = data[1]
	print(f"{len(files)} files found")
	return files

def GetCahceFiles(_path, _sqlFileData):
	#TODO - Need Error Handling.
	#Variables
	cacheFileData = []
	if not (os.path.exists(_path)): #Check if sqllite db exists
		raise Exception(f"Google FS Cahce Folder does not exist at {_path}")
	for root, dirs, file in os.walk(_path):
		for realFile in file:
			if not "." in realFile: #Needs to be improved upon
				#stable_id is offset by +1 from the cached file name. Example cached file 945 is stable_id 946
				sanitizedFile = int(realFile)
				sanitizedFile += 1
				if sanitizedFile  in _sqlFileData.keys():
					cacheFilePath = os.path.join(root,realFile)
					md5 = GetMD5Hash(cacheFilePath)
					cacheFileData.append({"stableId": sanitizedFile,"cacheFileName": realFile, "origFileName": _sqlFileData[sanitizedFile], "cacheFilePath": cacheFilePath, "cacheDirectory": root, "md5Hash": md5})
	return cacheFileData

def WriteCSV(_path, _cacheFileData):
	if not (os.path.exists(_path)): #Check if sqllite db exists
		raise Exception(f"Output path does not exist {_path}")
	
	headers = ['stable_id','cache_filename','original_filename','cache_file_path', 'cache_directory', 'md5']
	exportFileName = 'google-fs-filelist.csv'
	with open(f"{os.path.join(_path,exportFileName)}", 'w', encoding='UTF8', newline='') as f:
		allData = []
		writer = csv.writer(f)
		writer.writerow(headers)
		for entry in _cacheFileData:
			
			data = [entry['stableId'], entry['cacheFileName'], entry['origFileName'], entry['cacheFilePath'], entry['cacheDirectory'], entry['md5Hash']]
			writer.writerow(data)
		
		

def BackupFiles(_path, _cacheFileData):		
	if not (os.path.exists(_path)): #Check if sqllite db exists
		os.mkdir(_path)
	
	for cacheFile in _cacheFileData:
		shutil.copyfile(cacheFile['cacheFilePath'], os.path.join(_path, cacheFile['origFileName']))

def GetMD5Hash(_file):
	md5Hash = hashlib.md5()
	with open(_file,"rb") as f:
		# Read and update hash in chunks of 4K
		for byte_block in iter(lambda: f.read(4096),b""):
			md5Hash.update(byte_block)
		fileHash = md5Hash.hexdigest()
		return fileHash
	
def main(_args):
	#Arguments
	args = GetArgs(_args)
	sqlDb = args['db']
	cacheFiles = args["cache"]
	csvDest = args["csv"]
	backupPath = args["backup"]
	#Get SQL File Names
	sqlFileData = GetSqlData(sqlDb) 
	
	#Get Cache Files
	cacheFileData = GetCahceFiles(cacheFiles, sqlFileData)
	
	#Write Log file with cachefile names and original file names.
	WriteCSV(csvDest,cacheFileData)
	
	#Copy Data with Original File Names
	if (backupPath != None):
		BackupFiles(backupPath, cacheFileData)
	

if __name__ == "__main__":
	argv = sys.argv[1:]
	main(argv)