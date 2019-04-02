from __future__ import print_function

from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import client, file, tools
import io
from apiclient.http import MediaIoBaseDownload, MediaFileUpload
from subprocess import Popen
from multiprocessing import Pool, Value

# If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/drive'

# Max image size after which it should be compressed (bytes)
MX_IMG_SIZE = 1572864

# compressed image prefix
IMG_PREFIX = 'RS_'

# gobal drive service object
service = None

# global image counter
counter = None

def init(args):
    ''' store the counter for later use '''
    global counter
    counter = args

def medUpload(fileName, metadata, drive_service):
    media = MediaFileUpload(fileName, mimetype='image/jpeg', resumable=True)
    request = drive_service.files().create(media_body=media, body=metadata)
    response = None
    while response is None:
        status, response = request.next_chunk()
    print("UPLOADED [%s]." % fileName)

def medDownload(file, drive_service):
    file_id = file.get('id')
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO('imgs/'+file.get('name'), 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download {}% [{}].".format(int(status.progress() * 100), file.get('name')))
    
    proc = Popen(["convert", 'imgs/'+file.get('name'), "-sampling-factor", "4:2:0",
            "-strip", "-quality", "85", "-interlace", "JPEG", "-colorspace", "sRGB",
            'imgs/'+ IMG_PREFIX +file.get('name')])
    retCode = proc.wait(20)
    while retCode is None:
        retCode = proc.wait()
    print("COMPRESSED [{}] [{}].".format(retCode, file.get('name')))
    medUpload('imgs/'+ IMG_PREFIX +file.get('name'), {'name':  IMG_PREFIX +file.get('name'), 'parents': file.get('parents')}, drive_service)

def startCompression(fl):
    global counter
    global service
    
    with counter.get_lock():
        counter.value += 1
    
    print('%d. Found IMG: %s (%s) (%s)' % (counter.value, fl.get('name'), fl.get('id'), fl.get('size')))
    sz = int(fl.get('size'))
    # If size is greater than specified & not already compressed then download, compress and upload image then
    # Delete old image
    
    if sz > MX_IMG_SIZE and fl.get('name')[:3] != IMG_PREFIX :
        try:
            medDownload(fl, service)
        except Exception as err:
            print('Error occured: ', err)
        else:
            service.files().delete(fileId=fl.get('id')).execute()
            print('DELETED [{}]'.format(fl.get('name')))
            Popen(['rm', 'imgs/'+fl.get('name')])

def main():
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    global service
    global counter
    counter = Value('i', 0)

    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('drive', 'v3', http=creds.authorize(Http()))
    
    page_token = None

    while True:
        try:
            response = service.files().list(q="mimeType='image/jpeg'",
                                              spaces='drive',
                                              fields='nextPageToken, files(id, name, size, parents)',
                                              pageToken=page_token).execute()
        except Exception as err:
                print('ERROR OCCURED: ', err)
        # for fl in response.get('files', []):
        #     sz = int(fl.get('size'))
        #     img_count = img_count + 1
        #     # Total size of images greater than specified
        #     if sz > MX_IMG_SIZE and fl.get('name')[:3] != IMG_PREFIX :
        #         total_size += sz
        #     print('\rTotal size of images: ', int(total_size/(1024*1024)), 'MB', end='')
        # print('\n Total images: ', img_count)
        # mbs = int(total_size/(1024*1024))
        # ans = input('You should have around %dMB data available to download and upload images. Continue ? (y/N)' % (mbs + (mbs*0.85)) )
        
        with Pool(processes=10, initializer = init, initargs = (counter, )) as pool:
            try:
                pool.map(startCompression, response.get('files', []))
            except Exception as err:
                print('ERROR OCCURED: ', err)
        pool.join()
        print('Total images processed: ', counter.value)
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

if __name__ == '__main__':
    main()
