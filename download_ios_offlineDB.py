 # -*- coding: utf-8 -*-
# @Time    : 2022/5/9 下午5:36
# @Author  : Laura
# @File    : download_ios_offlineDB.py
# @Software: PyCharm Community Edition


from pymongo import *
import requests ,zipfile, io
import subprocess
import os
import json
import config

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import sys

gauth = GoogleAuth()
# Try to load saved client credentials
gauth.LoadCredentialsFile("mycreds.txt")
if gauth.credentials is None:
    # Authenticate if they're not there
    gauth.LocalWebserverAuth()
elif gauth.access_token_expired:
    # Refresh them if expired
    gauth.Refresh()
else:
    # Initialize the saved creds
    gauth.Authorize()
# Save the current credentials to a file
gauth.SaveCredentialsFile("mycreds.txt")
drive = GoogleDrive(gauth)


def get_offline_db_metadata(env):
    """
    Connect with MongoDB and call function DB_donwload_and_upload_to_googledrive

    :param env: connect with production env or staging env or testing only
    note: connect with staging server when env == testing

    :return: None
    """

    if env == "prod":
        # production
        client = MongoClient(config.prod_list['address'], username=config.prod_list['usr'],
                           password=config.prod_list['pwd'])
        # iosqa google drive production
        folderid = config.prod_list['folderid']
        DB_donwload_and_upload_to_googledrive(client,folderid,env)
    elif env =="staging":
        #staging
        client = MongoClient(config.stag_list['address'], username=config.stag_list['usr'],
                             password=config.stag_list['pwd'])
        # iosqa google drive staging
        folderid = config.stag_list['folderid']
        DB_donwload_and_upload_to_googledrive(client, folderid,env)
    elif env =="testing":
        # staging
        client = MongoClient(config.test_list['address'], username=config.test_list['usr'],
                             password=config.test_list['pwd'])
        # Laura folder for testing
        folderid = config.test_list['folderid']
        DB_donwload_and_upload_to_googledrive(client, folderid)
    else:
        print("environment doesn't exist. Please check ")

def DB_donwload_and_upload_to_googledrive(client,folderid,env=None):
    """
    Download offlineDB and decrypt it
    Upload offlineDB and revolver_key to google drive
    Call function send_message_to_slack if env is not None

    :param client: DB
    :param folderid: googlge drive folderid
    :param env: connect with production env or staging env or None(testing)

    :return: None
    """

    db = client.offlinedb_v90_ios
    offlineDB_utime = str(db.offlinedb_premium.find_one({"region": 'TW'}, sort=[('utime', -1)])['utime'])[:10]
    print(offlineDB_utime)
    try:

        # Create sub-folder
        sub_folder_name = offlineDB_utime+" DB"
        sub_folder_metadata = {'title': sub_folder_name, 'mimeType': 'application/vnd.google-apps.folder',
                               'parents': [{'id': folderid}]}
        sub_folder = drive.CreateFile(sub_folder_metadata)
        sub_folder.Upload()

        sub_folderid = sub_folder['id']
    except:
        print("Unexpected error:", sys.exc_info()[0])
    region_list = ['TW', 'HK', 'JP', 'KR', 'BR', 'TH','PH','MY']
    # region_list = ['HK']
    print(db.list_collection_names())
    dbkey_list ={}

    for x in region_list:
        print(x)
        offlineDB_metadata = db.offlinedb_premium.find_one({"region":x}, sort=[('utime', -1)])
        print(offlineDB_metadata)
        db_url = offlineDB_metadata['url']
        unzip_pwd = str(offlineDB_metadata['utime'])[:10]+ str(offlineDB_metadata['fcsums'])
        db_version = str(offlineDB_metadata['version'])
        db_key = offlineDB_metadata['key']
        print('Downloading started')
        req = requests.get(db_url)
        print('Downloading Completed')

        # extracting the zip file contents
        db_zipfile = zipfile.ZipFile(io.BytesIO(req.content))
        db_unzip = db_zipfile.extractall(path="./", pwd=unzip_pwd.encode())
        file_name=x+"_v"+db_version+".wcdb"
        print(file_name)

        path = os.getcwd() + '/'
        list_files = subprocess.run(["mv", path+"data", path+"data.lzma"])
        list_files = subprocess.run(["unlzma", "-d", path+"data.lzma"])
        list_files = subprocess.run(["mv", path+"data",
                                     path+file_name])
        key_name = x+"_v"+db_version
        dbkey_list[key_name] = db_key

        try:
            file = drive.CreateFile({"parents": [{"kind": "drive#fileLink", "id": sub_folderid}]})
            file.SetContentFile(file_name)
            file.Upload()

            print("DB is uploaded to google drive")
        except:
            print("Unexpected error:", sys.exc_info()[0])

    with open('revolver_key.txt', 'w') as f:
        f.write(json.dumps(dbkey_list))
    try:
        file = drive.CreateFile({"parents": [{"kind": "drive#fileLink", "id": sub_folderid}]})
        file.SetContentFile('revolver_key.txt')
        file.Upload()
        print("revolver_key.txt is uploaded to google drive")
        # env is None when testing the script
        if env is not None:
            # metion Cat
            send_message_to_slack(mention=config.mentioner_ID, envDB=env, google_drive=folderid)
    except:
        print("Unexpected error:", sys.exc_info()[0])
        # metion Laura
        send_message_to_slack(mention=config.owner_ID, envDB=env,result="fail")

def send_message_to_slack(envDB,mention=None,result=None, google_drive=None):
    """
    send result to slack

    :param envDB: connect with production env or staging env or testing only
    :param mention: meto\ion someone in slack
    :param result: Get fail if anything go wrong

    :return:None
    """
    # send message to slack channel qa-whoscall_app
    s_url = config.slack_webhook

    dict_headers = {'Content-type': 'application/json'}
    success_image = "https://i.imgur.com/7xH9GwQ.jpeg"
    fail_image ="https://memeprod.ap-south-1.linodeobjects.com/user-template/197cfd0c386bc60b694b984280f8259f.png"

    message = "Hi {}\nWhoscall iOS {} DB is already uploaded to Google Drive.\nPlease update revolver key in Apple Notes.\nGoogle Drive link:https://drive.google.com/drive/folders/{}\n{}".format(mention,envDB,google_drive,success_image)
    if result == "fail":
        message = "Hi {}\nWhoscall iOS {} DB is uploaded failed.\nPlease check it.\n{}".format(mention,envDB,fail_image)

    dict_payload = {
        "text": message}
    json_payload = json.dumps(dict_payload)

    rtn = requests.post(s_url, data=json_payload, headers=dict_headers)
    print(rtn.text)
    return None


if __name__ == "__main__":
    # prod,staging,testing
    if len(sys.argv) <2:
        print('no argument[env]\n Please enter pro/staging/testing.')
    get_offline_db_metadata(env=sys.argv[1])

