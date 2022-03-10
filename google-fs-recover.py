import os
import sqlite3
import sys
import argparse
import csv
import shutil
import hashlib
import blackboxprotobuf
from datetime import datetime
import time

def GetArgs(_args):#Get cmdline opts
	allArgs = argparse.ArgumentParser()
	
	allArgs.add_argument("-p","--path", required=True, help="Path of GoogleFS")
	allArgs.add_argument("-csv","--csv", required=False, help="Destination of CSV output")
	allArgs.add_argument("-b","--backup", required=False, help="Copy files to new location with new file names. Output path.")
	allArgs.add_argument("-i","--interactive", required=False, help="Interactive mode")
	
	return vars(allArgs.parse_args())

def GetSqlData(_path):
	#TODO - Need Error Handling. 
	#Variables
	selectStr = "SELECT item_stable_id, value from item_properties where key = 'content-entry'"
	files = []
	if not (os.path.exists(_path)): #Check if sqllite db exists
		raise Exception(f"SQLlite database does not exist at {_path}")
	#sqllite logic
	conn = sqlite3.connect(_path)
	cur = conn.cursor()
	for data in cur.execute(selectStr):
		files.append({"stableId": data[0], "protobufValue": data[1]})
	return files

def GetOrigFileNames(_path, _sqlFileData):
	#TODO - Need Error Handling.
	if not (os.path.exists(_path)): #Check if sqllite db exists
			raise Exception(f"SQLlite database does not exist at {_path}")
	conn = sqlite3.connect(_path)
	cur = conn.cursor()	
	count = 0
	#Variables
	files = []
	for entry in _sqlFileData:
		selectStr = f"SELECT stable_id, local_title, trashed, is_owner from items where id = '{entry['itemsId']}'"
		for data in cur.execute(selectStr):
			entry["origFileName"] = data[1]
			entry["trashed"] = data[2]
			entry["isOwner"] = data[3]
			count += 1
	print(f"{count} files found in SQLite DB")
	return _sqlFileData

def GetCacheFiles(_path, _sqlFileData):
	#TODO - Need Error Handling.
	#Variables
	cacheFileData = []
	count = 0
	if not (os.path.exists(_path)): #Check if sqllite db exists
		raise Exception(f"Google FS Cahce Folder does not exist at {_path}")
	for root, dirs, file in os.walk(_path):
		for realFile in file:
			for sqlEntry in _sqlFileData:
				if str(realFile) == str(sqlEntry["cacheFilename"]):
					count += 1
					cacheFilePath = os.path.join(root,realFile)
					md5 = GetMD5Hash(cacheFilePath)
					cacheFileData.append({"stableId": sqlEntry["stableId"],"cacheFilename": sqlEntry["cacheFilename"], "origFilename": sqlEntry["origFileName"], "cacheFilePath": cacheFilePath, "cacheDirectory": root, "md5Hash": md5, "trashed": bool(sqlEntry['trashed']), "isOwner": bool(sqlEntry["isOwner"]) })
	print(f"{len(cacheFileData)} files found in cache folder")					
	return cacheFileData

def WriteCSV(_path, _cacheFileData, _googleIdentifier):
	if not (os.path.exists(_path)): #Check if sqllite db exists
		raise Exception(f"Output path does not exist {_path}")
	count = 0
	headers = ['stable_id','cache_filename','original_filename','creation_time','modification_time','cache_file_path', 'cache_directory', 'md5', 'trashed', 'is_owner'] #write the headers for the csv
	timestamp = datetime.now()
	unixtime = time.mktime(timestamp.timetuple())
	unixtime = f"{unixtime}"
	unixtime = unixtime.split(".")
	unixtime = unixtime[0]
	formatedTimeStamp = timestamp.strftime("%Y%m%d%H%M%S_%f")
	exportFileName = f'google-fs-filelist-{_googleIdentifier}-{unixtime}.csv'
	with open(f"{os.path.join(_path,exportFileName)}", 'w', encoding='UTF8', newline='') as f:
		allData = []
		writer = csv.writer(f)
		writer.writerow(headers)
		for entry in _cacheFileData:
			creationTime = os.path.getctime(entry['cacheFilePath'])
			creationTime = time.ctime(creationTime)
			modificationTime = os.path.getmtime(entry['cacheFilePath'])
			modificationTime = time.ctime(modificationTime)
			data = [entry['stableId'], entry['cacheFilename'], entry['origFilename'], creationTime, modificationTime, entry['cacheFilePath'], entry['cacheDirectory'], entry['md5Hash'], entry['trashed'], entry['isOwner']]
			writer.writerow(data)
			count += 1
	print(f"{count} rows written to csv")
			

def BackupFiles(_path, _cacheFileData):		
	if not (os.path.exists(_path)): #Check if sqllite db exists
		os.mkdir(_path)
	count = 0
	for cacheFile in _cacheFileData:
		if not os.path.exists(cacheFile['cacheFilePath']):
			print(f"File Not Found: {cacheFile['cacheFilePath']}")
		else:
			detinationFile = f"{os.path.join(_path, cacheFile['origFilename'])}"
			if os.path.exists(detinationFile):
				detinationFile = f"{detinationFile}_{cacheFile['cacheFilename']}"
			shutil.copyfile(cacheFile['cacheFilePath'], detinationFile)
			count += 1
	print(f"{count} files copied to backup folder")

def GetMD5Hash(_file):
	md5Hash = hashlib.md5()
	with open(_file,"rb") as f:
		# Read and update hash in chunks of 4K
		for byte_block in iter(lambda: f.read(4096),b""):
			md5Hash.update(byte_block)
		fileHash = md5Hash.hexdigest()
		return fileHash

def DecodeProtoBuf(_sqlFileData):
	parsedData = []
	for file in _sqlFileData:
		missingKeys = False
		message,typedefs = blackboxprotobuf.decode_message(file["protobufValue"])
		if "1" in message.keys():
			file["cacheFilename"] = message["1"]
		else:
			missingKeys = True
		if "3" in message.keys():
			file["itemsId"] = message["3"].decode('utf-8')
		else:
			missingKeys = True
		if "4" in message.keys():
			file["fileSize"] = message["4"]
		else:
			missingKeys = True
		
		if missingKeys == False:
			parsedData.append(file)
		
	return parsedData
		
	
def main(_args):
	#Arguments
	args = GetArgs(_args)
	googlePath = args["path"] #Get path of google fs
	googleIdentifier = googlePath.split('\\')
	googleIdentifier = googleIdentifier[len(googleIdentifier) -1]
	sqlDb = os.path.join(googlePath, "metadata_sqlite_db")
	cacheFiles = os.path.join(googlePath, "content_cache")
	csvDest = args["csv"]
	backupPath = args["backup"]
	#Get SQL File Names
	sqlFileData = GetSqlData(sqlDb) 
	
	#Decode Proto buffer
	sqlDecodedData = DecodeProtoBuf(sqlFileData)
	
	#GetOrigFileNames
	sqlDecodedDataFiles = GetOrigFileNames(sqlDb, sqlDecodedData)
	
	#Get Cache Files
	cacheFileData = GetCacheFiles(cacheFiles, sqlDecodedDataFiles)
	
	#Write Log file with cachefile names and original file names.
	if (csvDest != None):
		WriteCSV(csvDest,cacheFileData, googleIdentifier)
	
	#Copy Data with Original File Names
	if (backupPath != None):
		BackupFiles(backupPath, cacheFileData)
	

if __name__ == "__main__":
	argv = sys.argv[1:]
	main(argv)