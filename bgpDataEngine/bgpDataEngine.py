'''
Created on Oct 9, 2015

@author: akshah
'''
from __future__ import print_function
import MySQLdb as pymysql
from MySQLdb.connections import OperationalError
import pycurl
import configparser
from contextlib import closing
import os
import time
import ipaddress
import hashlib
import random
import netaddr
import threading
import traceback
import subprocess
from queue import Queue
from multiprocessing import Queue as mQueue
from urllib.request import urlopen
from customUtilities.logger import logger
# from os import listdir
# from os.path import isfile, join
from customUtilities.processPool import processPool

from multiprocessing import current_process, cpu_count


class bgpDataEngine(object):
    '''
    Class to access BGPmon and RouteViews archive, push data to MySQL and query it.
    '''

    def __init__(self, configfile='./conf/bgpDataEngine.conf'):
        '''
        Constructor
        '''
        self.configfile = configfile
        # Load config file
        self.config = configparser.ConfigParser()
        try:
            self.config.sections()
            self.config.read(configfile)
        except:
            print('Missing config: ' + configfile)

        self.logger = logger(self.config['DEFAULTDIRS']['logFile'])
        self.logger.info("***Initializing BGP data engine.***")

        self.numThreads = int(self.config['EXEC']['numThreads'])
        if not self.numThreads > 0:
            self.logger.error('Please use valid number of threads.')
            print('Please use valid number of threads. Not valid: ' + self.numThreads)
            exit(1)
        if self.numThreads > cpu_count():
            self.numThreads = cpu_count()
            self.logger.warn('Specified numThreads value is too high, using ' + str(self.numThreads) + ' threads only.')

        self.dbFlag = False
        try:
            self.db = pymysql.connect(host=self.config['MySQL']['serverIP'],
                                      port=int(self.config['MySQL']['serverPort']),
                                      user=self.config['MySQL']['user'],
                                      passwd=self.config['MySQL']['password'],
                                      db=self.config['MySQL']['dbname'])
            self.dbFlag = True
            self.logger.info(
                'Test connection to MySQL server on ' + self.config['MySQL']['serverIP'] + ":" + self.config['MySQL'][
                    'serverPort'] + ' successful.')
            self.db.close()
        except:
            self.logger.warn('DB connection not established. load2DB() may not work!.')

        self.bgpmoncollectors = ['bgpmon']
        self.rvcollectors = ['route-views2', 'route-views4', 'route-views3', 'route-views6', 'route-views.eqix',
                             'route-views.isc', 'route-views.kixp', 'route-views.jinx' \
            , 'route-views.linx', 'route-views.nwax', 'route-views.telxatl', 'route-views.wide', 'route-views.sydney',
                             'route-views.saopaulo', 'route-views.sg', \
                             'route-views.perth', 'route-views.sfmix', 'route-views.soxrs']
        self.ripecollectors = ['rrc00', 'rrc01', 'rrc02', 'rrc03', 'rrc04', 'rrc05', 'rrc06', 'rrc07', 'rrc08', 'rrc09',
                               'rrc10', 'rrc11', 'rrc12', 'rrc13', 'rrc14', 'rrc15', 'rrc16']

        #Booleans to keep track of which archive we want to query.
        #If user does not specify anything, by default all will be used to pull data from.
        self.accessToBGPMonArchive = True
        self.accessToRVArchive = True
        self.accessToRipeArchive = True

        self.peer_list = []  # Peer list to keep track of unique peers BGP feeds are seen from
        self.peer_list_lock = threading.Lock()
        self.filesDownloaded = []
        self.tableNameList = []
        self.tableNameListLock = threading.Lock()
        # self.toPushDataLocks={}

        # Queue to hold filelist
        self.rangeQueue = Queue()
        self.queueRV = Queue()  # Queue to hold list of URLs to be downloaded

        self.messageQueue = mQueue()

    def downloadFile(self, url):
        try:
            f = urlopen(url)
            print("Downloading: " + url)

            # Open our local file for writing
            with open(os.path.basename(url), "wb") as local_file:
                local_file.write(f.read())
            print("Finished downloading: " + url)
        except:
            print("Download Error")
            exit(1)

    def __downloadFileWorker(self):
        while True:
            item = self.queueRV.get()
            if item == None:
                self.queueRV.task_done()
                break
            # print(item)
            try:
                tryCounter, collector, year, month, day, url = item.split('|')
            except:
                # Some weird queue item, skip it
                self.logger.error('Problem in splitting the queue item.')
                self.queueRV.task_done()
                continue

            tryCounter = int(tryCounter)
            if tryCounter == 3:
                # This URL was tried 3 times before, I will skip it.
                self.queueRV.task_done()
                continue

            if collector == "":
                collector = 'route-views2'

            # Open the url
            # Check if file already exists
            dowloadFileNameWithPath = self.dirpath + collector + '/' + year + '/' + month + '/' + day + '/' + collector + '.' + os.path.basename(
                url)
            #Download only 3 files
            if not ('.0000.' in dowloadFileNameWithPath or '.0800.' in dowloadFileNameWithPath or '.1600.' in dowloadFileNameWithPath):
                self.queueRV.task_done()
                continue

            if os.path.isfile(dowloadFileNameWithPath):
                # print('File already present')
                self.queueRV.task_done()
                continue

            try:
                f = urlopen(url)
                # print("Downloading: " + url)

                # Open our local file for writing
                os.makedirs(os.path.dirname(dowloadFileNameWithPath), exist_ok=True)
                with open(dowloadFileNameWithPath, "wb") as local_file:
                    local_file.write(f.read())
                self.filesDownloaded.append(dowloadFileNameWithPath)
                if tryCounter > 0:
                    self.logger.info('File ' + url + ' downloaded successfully in ' + str(tryCounter + 1) + ' tries.')
            # handle errors
            except:
                self.logger.warn("Download Error for " + url)
                tryCounter += 1
                self.queueRV.put(str(tryCounter) + '|' + collector + '|' + url)
            finally:
                self.queueRV.task_done()

    def getRange(self, datatype, start, end, load2db=True, collectors=[]):
        self.dirpath = self.config['DEFAULTDIRS']['MRTDir']
        if os.path.exists(self.dirpath):
            self.logger.info('Using ' + self.dirpath + ' for downloading MRT Files')
        else:
            self.logger.error('Please use valid path for MRT download.')
            print('Please use valid path for MRT download. Not valid: ' + self.dirpath)
            exit(1)

        try:
            if(len(start)>14 or len(start)>14):
                print('Date ranges dont look correct: {0} and {1}'.format(start,end))
                self.logger.error('Date ranges dont look correct: {0} and {1}'.format(start,end))
            intStart=int(start)
            intEnd=int(end)
        except:
            self.logger.error('Date ranges dont look correct: {0} and {1}'.format(start,end))

        if (len(collectors) > 0):
            # Prepare respective list for the archives
            accessToBGPMonArchive = False
            accessToRVArchive = False
            accessToRipeArchive = False
            localbgpmoncollectors=[]
            localrvcollectors=[]
            localripecollectors=[]
            for collc in collectors:
                if collc in self.bgpmoncollectors:
                    localbgpmoncollectors.append(collc)
                    accessToBGPMonArchive = True
                elif collc in self.rvcollectors:
                    localrvcollectors.append(collc)
                    accessToRVArchive = True
                elif collc in self.ripecollectors:
                    localripecollectors.append(collc)
                    accessToRipeArchive = True
                else:
                    self.logger.error('Collector not valid: ' + str(collc))
                    print('Collector not valid: ' + str(collc))
                    exit(1)
            if accessToBGPMonArchive:
                if not self.accessToBGPMonArchive:
                    self.logger.warn('Ignoring accessToBGPMonArchive boolean, since BGPmon collector is requested.')
                self.logger.info('Will connect to BGPmon archive.')
                self.getRangeFromBGPmon(datatype, start, end,collectors=localbgpmoncollectors)
            if accessToRVArchive:
                if not self.accessToRVArchive:
                    self.logger.warn('Ignoring accessToRVArchive boolean, since RouteViews collector is requested.')
                self.logger.info('Will connect to RouteViews archive.')
                self.getRangeFromRV(datatype, start, end,collectors=localrvcollectors)
            if accessToRipeArchive:
                if not self.accessToRipeArchive:
                    self.logger.warn('Ignoring accessToRipeArchive boolean, since RIPE collector is requested.')
                self.logger.info('Will connect to RIPE archive.')
                self.getRangeFromRipe(datatype, start, end,collectors=localripecollectors)
        else:
            # User has not given specific collectors, use all.
            if self.accessToBGPMonArchive:
                self.getRangeFromBGPmon(datatype, start, end)
            if self.accessToRVArchive:
                self.getRangeFromRV(datatype, start, end)
            if self.accessToRipeArchive:
                self.getRangeFromRipe(datatype, start, end)
        self.logger.info('All downloads finished.')
        if load2db:
            self.logger.info('Will push data to MySQL.')
            self.load2DB()

    def getRangeFromRV(self, datatype, start, end,collectors=[]):
        self.logger.info('Preparing to pull data from RouteViews archive.')
        if len(collectors) == 0:
            collectors = self.rvcollectors
        ldatatype = datatype.lower()
        if (ldatatype != 'ribs' and ldatatype != 'updates'):
            self.logger.error('Incorrect data type. Use ribs or updates.')
            return 1
            exit(1)
        if (int(start) > int(end)):
            print('Start time cannot be before End time.')
            exit(1)

        syear = start[:4]
        smonth = start[4:6]
        sday = start[6:8]
        eyear = end[:4]
        emonth = end[4:6]
        eday = end[6:8]
        if (syear != eyear):
            print('Please use start and end range in same year.')
            self.logger.error('Please use start and end range in same year.')
            exit(1)
        if (smonth != emonth):
            print('Please use start and end range in same month.')
            self.logger.error('Please use start and end range in same month.')
            exit(1)
        if (len(start) > 8 or len(end) > 8):
            self.logger.warn('Note, getRange functions only fetch per-day. Ignoring hour:min:sec info.')
            print('Note, getRange functions only fetch per-day. Ignoring hour:min:sec info.')

        # print('Fetch '+ldatatype+' from '+start+' to '+end)
        self.queueRV.queue.clear()

        threadPool = []
        for i in range(30):
            t = threading.Thread(target=self.__downloadFileWorker)
            t.daemon = True  # Thread dies when main thread (only non-daemon thread) exits.
            t.start()
            threadPool.append(t)

        dateRange = []
        if (sday == eday):
            dateRange.append(start)
        else:
            for dayiter in range(int(sday), int(eday) + 1):
                TmpDay = str(dayiter)
                if len(TmpDay) < 2:
                    TmpDay = '0' + TmpDay
                currStart = TmpDay
                dateRange.append(syear + smonth + currStart)

        self.logger.info('Initializing MRT downloads.')
        totalFileCounter = 0
        for dr in dateRange:
            year = dr[:4]
            month = dr[4:6]
            day = dr[6:8]
            for collector in collectors:
                url = "http://archive.routeviews.org/"
                indexFile = collector + '_' + dr + 'index.download'
                fileNames = []
                try:
                    if collector == 'route-views2':
                        collector = ""  # Since rv2 URL does not have the name of collector in it.
                    url = url + collector + '/bgpdata/' + year + '.' + month + '/' + datatype.upper()
                    f = urlopen(url)
                    # print("Downloading: " + url)

                    # Open our local file for writing
                    with open(indexFile, "wb") as local_file:
                        local_file.write(f.read())
                # handle errors
                except:
                    self.logger.warn("Download Error for index file: " + url)
                    continue
                tryCounter = 0  # Threads will increment this if needed to try again
                with open(indexFile, 'r') as readFile:
                    for line in readFile:
                        if year + month + day in line:
                            vals = line.split('href=\"')
                            vals2 = vals[1].split('\">')
                            fileNames.append(str(
                                tryCounter) + '|' + collector + '|' + year + '|' + month + '|' + day + '|' + url + '/' +
                                             vals2[0])
                os.remove(indexFile)

                for fileURL in fileNames:
                    totalFileCounter += 1
                    self.queueRV.put(fileURL)

        self.logger.info(str(totalFileCounter) + ' file(s) in queue, waiting for download to finish.')

        qSizeVal = self.queueRV.qsize()
        checkVal = qSizeVal
        while qSizeVal > 1000:
            if abs(qSizeVal - checkVal) < self.numThreads + 100:
                self.logger.info(str(qSizeVal) + ' MRT files remaining to download.')
                checkVal -= 1000
            qSizeVal = self.queueRV.qsize()

        for i in range(30):
            self.queueRV.put(None)
        self.queueRV.join()
        for t in threadPool:
            t.join()
        del threadPool[:]

        self.logger.info('Download from RouteViews finished.')

    def getRangeFromRipe(self, datatype, start, end,collectors=[]):
        self.logger.info('Preparing to pull data from RIPE archive.')
        if len(collectors) == 0:
            collectors = self.ripecollectors
        ldatatype = datatype.lower()
        if (ldatatype != 'ribs' and ldatatype != 'updates'):
            self.logger.error('Incorrect data type. Use ribs or updates.')
            return 1
            exit(1)
        if (ldatatype == 'ribs'):
            ldatatype = 'bview' #Ripe calls RIBS bview

        if (int(start) > int(end)):
            print('Start time cannot be before End time.')
            exit(1)

        syear = start[:4]
        smonth = start[4:6]
        sday = start[6:8]
        eyear = end[:4]
        emonth = end[4:6]
        eday = end[6:8]
        if (syear != eyear):
            print('Please use start and end range in same year.')
            exit(1)
        if (smonth != emonth):
            print('Please use start and end range in same month.')
            exit(1)
        if (len(start) > 8 or len(end) > 8):
            print('This function only fetches per-day. Ignoring hour:min:sec info.')

        # print('Fetch '+ldatatype+' from '+start+' to '+end)
        self.queueRV.queue.clear()  # Using same queue

        threadPool = []
        for i in range(30):
            t = threading.Thread(target=self.__downloadFileWorker)
            t.daemon = True  # Thread dies when main thread (only non-daemon thread) exits.
            t.start()
            threadPool.append(t)

        dateRange = []
        if (sday == eday):
            dateRange.append(start)
        else:
            for dayiter in range(int(sday), int(eday) + 1):
                TmpDay = str(dayiter)
                if len(TmpDay) < 2:
                    TmpDay = '0' + TmpDay
                currStart = TmpDay
                dateRange.append(syear + smonth + currStart)

        self.logger.info('Initializing MRT downloads.')
        totalFileCounter = 0
        for dr in dateRange:
            year = dr[:4]
            month = dr[4:6]
            day = dr[6:8]
            for collector in collectors:
                url = "http://data.ris.ripe.net/"
                indexFile = collector + '_' + dr + 'index.download'
                fileNames = []
                try:
                    url = url + collector + '/' + year + '.' + month + '/'
                    f = urlopen(url)
                    # print("Downloading: " + url)

                    # Open our local file for writing
                    with open(indexFile, "wb") as local_file:
                        local_file.write(f.read())
                # handle errors
                except:
                    self.logger.warn("Download Error for index file: " + url)
                    continue
                tryCounter = 0  # Threads will increment this if needed to try again
                with open(indexFile, 'r') as readFile:
                    for line in readFile:
                        if ldatatype + "." + year + month + day in line:
                            vals = line.split('href=\"')
                            vals2 = vals[1].split('\">')
                            fileNames.append(str(
                                tryCounter) + '|' + collector + '|' + year + '|' + month + '|' + day + '|' + url + '/' +
                                             vals2[0])
                os.remove(indexFile)

                for fileURL in fileNames:
                    totalFileCounter += 1
                    self.queueRV.put(fileURL)

        self.logger.info(str(totalFileCounter) + ' file(s) in queue, waiting for download to finish.')

        qSizeVal = self.queueRV.qsize()
        checkVal = qSizeVal
        while qSizeVal > 1000:
            if abs(qSizeVal - checkVal) < self.numThreads + 100:
                self.logger.info(str(qSizeVal) + ' MRT files remaining to download.')
                checkVal -= 1000
            qSizeVal = self.queueRV.qsize()

        for i in range(30):
            self.queueRV.put(None)
        self.queueRV.join()
        for t in threadPool:
            t.join()
        del threadPool[:]

        self.logger.info('Download from RIPE finished.')

    def _lastDayOfMonth(self, year, month):
        thirtyOneList = ['01', '03', '05', '07', '08', '10', '12']
        if (month in thirtyOneList):
            return '31'
        elif (month == '02'):
            if (int(year) % 4 == 0):
                return '29'
            else:
                return '28'
        else:
            return '30'

    def _loadRangeWorker(self):
        while True:
            item = self.rangeQueue.get()
            if item == None:
                self.rangeQueue.task_done()
                break
            # print('Got '+item)
            collector, ldatatype, start, end = item.split('|')
            self.logger.info('Collector: ' + collector + ' | Start: ' + start + ' | End: ' + end)
            # print('Collector: '+collector+' | Start: '+start+' | End: '+end)
            #Dir structure : collector/year/month/
            bgpFile = self.dirpath + '/' + collector + '/' + start[:4] + '/' + start[4:6] + '/' + collector +'.' + ldatatype + '.' + start[:8] + '.' + start[8:12] + '.mrt'
            os.makedirs(os.path.dirname(bgpFile), exist_ok=True)
            self.filesDownloaded.append(bgpFile)
            with open(bgpFile, 'wb') as f:
                try:
                    # buffer = BytesIO()
                    c = pycurl.Curl()
                    # c.setopt(c.URL, 'http://pycurl.sourceforge.net/')
                    url = 'http://bgpmon.io/archive/mrt/' + collector + '/' + ldatatype + '?start=' + start + '&end=' + end
                    print(url)
                    c.setopt(c.URL, url)
                    c.setopt(c.WRITEDATA, f)
                    c.perform()
                    c.close()
                except:
                    self.logger.warn('Empty Range: ' + start + ' to ' + end)
                    #Should delete the empty file
                    #os.remove(bgpFile)
            self.rangeQueue.task_done()

    def getTableName(self, type, peer, day):
        peerU = peer
        if ipaddress.IPv4Address(peer):
            peerU = peer.replace('.', '_')
        elif ipaddress.IPv6Address(peer):
            peerEx = ipaddress.IPv6Address(peer).exploded  # Full representation
            peerU = peerEx.replace(':', '_')
        else:
            self.logger.error('Provided Peer IP does not look right: ' + peer)
            print('Provided Peer IP does not look right: ' + peer)
            exit(1)

        tableName = type + '_d' + day + '_p' + peerU
        return tableName

    def getRangeFromBGPmon(self, datatype, start, end,collectors=[]):
        self.logger.info('Preparing to pull data from BGPmon archive.')
        if len(collectors) == 0:
            collectors = self.bgpmoncollectors
        ldatatype = datatype.lower()
        if (ldatatype != 'ribs' and ldatatype != 'updates'):
            self.logger.error('Incorrect data type. Use ribs or updates.')
            return 1
        #if (start == end):
        #    print('Start and End cannot be same.')
        #    exit(1)
        if (int(start) > int(end)):
            print('Start time cannot be before End time.')
            exit(1)

        if len(start)==6:
            start=start + '000000'
        if len(end)==6:
            end=end + '235959'
        syear = start[:4]
        smonth = start[4:6]
        sday = start[6:8]
        eyear = end[:4]
        emonth = end[4:6]
        eday = end[6:8]
        if (syear != eyear):
            print('Please use start and end range in same year.')
            exit(1)
        if (smonth != emonth):
            print('Please use start and end range in same month.')
            exit(1)

        self.rangeQueue.queue.clear()
        threadPool = []
        for i in range(30):
            t = threading.Thread(target=self._loadRangeWorker)
            t.daemon = True  # Thread dies when main thread (only non-daemon thread) exits.
            t.start()
            threadPool.append(t)

        for collector in collectors:
            if (sday == eday):
                self.rangeQueue.put(collector + '|' + ldatatype + '|' + start + '|' + end )
                #print(ldatatype + '|' + start + '|' + end)
            else:
                currStart = start
                endHourStr = end[8:]
                for dayiter in range(int(sday), int(eday) + 1):
                    TmpDay = str(dayiter)
                    TmpDayNext = str(dayiter + 1)
                    if len(TmpDay) < 2:
                        TmpDay = '0' + TmpDay
                    if len(TmpDayNext) < 2:
                        TmpDayNext = '0' + TmpDayNext
                    if TmpDay == eday:
                        currEnd = eyear + emonth + TmpDay + endHourStr
                    else:
                        currEnd = eyear + emonth + TmpDay + '235959'
                    self.rangeQueue.put(collector + '|' + ldatatype + '|' + currStart + '|' + currEnd)
                    #print(ldatatype + '|' + currStart + '|' + currEnd)
                    currStart = eyear + emonth + TmpDayNext + '000000'

        qSizeVal = self.rangeQueue.qsize()
        checkVal = qSizeVal
        while qSizeVal > 1000:
            if abs(qSizeVal - checkVal) < self.numThreads + 100:
                self.logger.info(str(qSizeVal) + ' MRT files remaining to download.')
                checkVal -= 1000
            qSizeVal = self.rangeQueue.qsize()

        for i in range(30):
            self.rangeQueue.put(None)
        self.rangeQueue.join()
        for t in threadPool:
            t.join()
        del threadPool[:]
        self.logger.info('Range ' + start + ' to ' + end + ' files fetched.')

    def getMonth(self, datatype, year, month, load2db=True,collectors=[]):
        if (len(month) != 2 or len(year) != 4):
            self.logger.error('Incorrect format. Use YYYY MM.')
            return 1
        daystart = '01'
        dayend = self._lastDayOfMonth(year, month)
        start = year + month + daystart  # + '000000'
        end = year + month + dayend  # + '235959'
        self.getRange(datatype, start, end, load2db=load2db,collectors=collectors)

    def _checkIfTableExists(self, table):
        tryCounter = 0
        while True:
            try:
                dblocal = pymysql.connect(host=self.config['MySQL']['serverIP'],
                                          port=int(self.config['MySQL']['serverPort']),
                                          user=self.config['MySQL']['user'],
                                          passwd=self.config['MySQL']['password'],
                                          db=self.config['MySQL']['dbname'])
                with closing(dblocal.cursor()) as cur:
                    try:
                        # cur.execute("SHOW TABLES LIKE \'%s\';",table)
                        query = "SHOW TABLES LIKE \'{0}\';".format(table)
                        cur.execute(query)
                        retval = cur.fetchone()
                    except:
                        raise Exception('Show tables failed')
                dblocal.close()
                if retval:
                    # print('Table '+table+' does not exists')
                    return True
                else:
                    # print('Table '+table+' does not exists')
                    return False
            except OperationalError:
                if tryCounter < 1000:
                    self.logger.error('No DB Connection available.. Will try again..')
                    # time.sleep(0.01)
                    tryCounter += 1
                    continue
                else:
                    self.logger.error('DB connection error.')
                    break
        return False

    def _createPeerListTable(self):
        tryCounter = 0
        while True:
            try:
                dblocal = pymysql.connect(host=self.config['MySQL']['serverIP'],
                                          port=int(self.config['MySQL']['serverPort']),
                                          user=self.config['MySQL']['user'],
                                          passwd=self.config['MySQL']['password'],
                                          db=self.config['MySQL']['dbname'])
                with closing(dblocal.cursor()) as cur:
                    try:
                        query = "CREATE TABLE peer_list ( \
                                    PeerIP VARCHAR(255),  \
                                    INDEX(PeerIP));"
                        cur.execute(query)
                        query = "ALTER TABLE peer_list \
                                ADD UNIQUE INDEX peeridx (PeerIP);"
                        cur.execute(query)
                        dblocal.commit()
                        self.logger.info('Created peer_list table.')
                        # print('Created '+table+' table.')
                        dblocal.close()
                        return True
                    except:
                        self.logger.error('Create table peer_list failed!')
                        return False
            except OperationalError:
                if tryCounter < 1000:
                    self.logger.error('No DB Connection available.. Will try again..')
                    # time.sleep(0.01)
                    tryCounter += 1
                    continue
                else:
                    self.logger.error('DB connection error.')
                    break
        return False

    def _createPeerDayTable(self):
        tryCounter = 0
        while True:
            try:
                dblocal = pymysql.connect(host=self.config['MySQL']['serverIP'],
                                          port=int(self.config['MySQL']['serverPort']),
                                          user=self.config['MySQL']['user'],
                                          passwd=self.config['MySQL']['password'],
                                          db=self.config['MySQL']['dbname'])
                with closing(dblocal.cursor()) as cur:
                    try:
                        query = "CREATE TABLE peer_day ( \
                                    PeerIP VARCHAR(255),  \
                                    Day VARCHAR(8),  \
                                    Updates INT NOT NULL,  \
                                    Ribs INT NOT NULL,  \
                                    INDEX(PeerIP));"
                        cur.execute(query)
                        dblocal.commit()
                        self.logger.info('Created peer_day table.')
                        # print('Created '+table+' table.')
                        dblocal.close()
                        return True
                    except:
                        self.logger.error('Create table peer_day failed!')
                        return False
            except OperationalError:
                if tryCounter < 1000:
                    self.logger.error('No DB Connection available.. Will try again..')
                    # time.sleep(0.01)
                    tryCounter += 1
                    continue
                else:
                    self.logger.error('DB connection error.')
                    break
        return False

    #Method to read a MRT file
    def readMRT(self,MRTfile):
        pid = os.fork()
        if pid == 0:
            try:
                lines = subprocess.check_output(["bgpdump", "-m", MRTfile], universal_newlines=True)
            except:
                self.logger.error('BGP file '+MRTfile+' could not be read properly.')
                print('BGP file '+MRTfile+' could not be read properly.')
                exit(0)

            for line in lines.split("\n"):
                line.rstrip('\n')
                if line.startswith("BGP") or line.startswith('TABLE'):  # Eliminate possibility of empty rows
                    pieces = line.split('|')
                    self.messageQueue.put(pieces)
            self.messageQueue.put(None)
            exit(0)
        else:
            return

    def load2DB(self, mrtFiles=[]):
        if len(mrtFiles) == 0:
            mrtFiles = self.filesDownloaded
        # print(mrtFiles)
        # print('In load2DB, '+str(len(mrtFiles))+' files to push.')
        self.logger.info('Preparing to push ' + str(len(mrtFiles)) + ' MRT file(s) to DB.')
        random.shuffle(mrtFiles)
        # Before starting to load MRT data check if PeerIP table exists
        if not self._checkIfTableExists('peer_list'):
            # Table does not exist. Create it.
            if not self._createPeerListTable():
                self.logger.error('There was create table error for peer_list.')
                print('There was create table error for peer_list.')
                exit(1)
        if not self._checkIfTableExists('peer_day'):
            # Table does not exist. Create it.
            if not self._createPeerDayTable():
                self.logger.error('There was create table error for peer_day.')
                print('There was create table error for peer_day.')
                exit(1)

        #processPoolObj = processPool(self.numThreads)
        #updatedTables = processPoolObj.runParallelWithPool(loadWorkerForProcessPool, mrtFiles, self.logger)
        updatedTables=[]
        for mrtFileName in mrtFiles:
            tables=loadWorkerForProcessPool(mrtFileName)
            for tb in tables:
                updatedTables.append(tb)

        # Add and drop index to keep only unique rows
        uniqueTables = []
        for tl in updatedTables:
            for tentry in tl:
                if tentry not in uniqueTables:
                    uniqueTables.append(tentry)
        self.logger.info('Prepared all processed tables.')

        # Manage Indexes
        self.logger.info('Prepared all Indexes.')
        processPoolObj = processPool(self.numThreads)
        retVals = processPoolObj.runParallelWithPool(manageDuplicateRows, uniqueTables, self.logger)
        # for table in uniqueTables:
        #    manageDuplicateRows(table)

        self.logger.info('Pushed all MRT file(s) to DB.')

    ###Methods to get data from MySQL DB

    def getUpdateMessages(self, peer, day):
        pid = os.fork()
        if pid == 0:
            tableName = self.getTableName('updates', peer, day)
            dblocal = pymysql.connect(host=self.config['MySQL']['serverIP'],
                                      port=int(self.config['MySQL']['serverPort']),
                                      user=self.config['MySQL']['user'],
                                      passwd=self.config['MySQL']['password'],
                                      db=self.config['MySQL']['dbname'])
            with closing(dblocal.cursor()) as cur:
                try:
                    # query="SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED ; \
                    #        select * from "+tableName+"; COMMIT ;"
                    query = "select * from " + tableName
                    cur.execute(query)
                    row = cur.fetchone()
                    while row is not None:
                        self.messageQueue.put(row)
                        row = cur.fetchone()
                except:
                    self.logger.error('Select to table ' + tableName + ' failed!')
                    print('Select to table ' + tableName + ' failed!')
            dblocal.close()
            self.messageQueue.put(None)
            # return pid
            exit(0)
        else:
            return


#####Non class functions
def manageDuplicateRows(table):
    tryCounter = 0
    config = configparser.ConfigParser()
    config.read('./conf/bgpDataEngine.conf')
    config.sections()
    while True:
        try:
            dblocal = pymysql.connect(host=config['MySQL']['serverIP'],
                                      port=int(config['MySQL']['serverPort']),
                                      user=config['MySQL']['user'],
                                      passwd=config['MySQL']['password'],
                                      db=config['MySQL']['dbname'])
            with closing(dblocal.cursor()) as cur:
                try:
                    query = "set session old_alter_table=1;"
                    cur.execute(query)
                    dblocal.commit()
                except:
                    traceback.print_exc()
                    print('Error in setting session for old-alter-table.')
                #
                #             with closing( dblocal.cursor() ) as cur:
                #                 try:
                #                     query="ALTER TABLE {0} \
                #                              ADD UNIQUE INDEX idx (MsgHash);".format(table)
                #                     cur.execute(query)
                #                     dblocal.commit()
                #                 except:
                #                     traceback.print_exc()
                #                     print('Error in creating indexes.')
                #             with closing( dblocal.cursor() ) as cur:
                #                 try:
                #                     query="ALTER TABLE {0} \
                #                              DROP INDEX idx;".format(table)
                #                     cur.execute(query)
                #                     dblocal.commit()
                #                 except:
                #                     traceback.print_exc()
                #                     print('Error in dropping indexes.')
            '''
            with closing( dblocal.cursor() ) as cur: 
                try:
                    query="ALTER TABLE {0} \
                             DROP INDEX msgTimeIndex;".format(table)
                    cur.execute(query)
                    dblocal.commit()
                except:
                    traceback.print_exc()
                    print('Error in dropping msgTimeIndex indexes.')

            with closing( dblocal.cursor() ) as cur: 
                try:
                    query="ALTER TABLE {0} \
                             DROP INDEX prefixIPIndex;".format(table)
                    cur.execute(query)
                    dblocal.commit()
                except:
                    traceback.print_exc()
                    print('Error in dropping prefixIPIndex indexes.')
            '''
            idsToDelete = []
            with closing(dblocal.cursor()) as cur:
                try:
                    query = "SELECT group_concat(ID), count(*) as count from {0} GROUP BY MsgHash HAVING count > 1;".format(
                        table)
                    cur.execute(query)
                    rowAll = cur.fetchone()
                    while rowAll:
                        row = rowAll[0]
                        entries = row.split(',')
                        for itr in range(len(entries)):
                            if itr > 0:
                                tmp = []
                                tmp.append(entries[itr])
                                idsToDelete.append(tmp)
                                # idsToDelete+='\''+str(entries[itr])+'\''+','
                        rowAll = cur.fetchone()
                except:
                    traceback.print_exc()
                    print('Error in getting duplicate IDS')
            # print(str(len(idsToDelete))+' entries to delete!')
            # print(idsToDelete[2])
            with closing(dblocal.cursor()) as cur:
                try:
                    queryList = []
                    # for ids in idsToDelete:
                    query = "DELETE FROM " + table + " WHERE ID = %s"
                    #    queryList.append(query)
                    # print('Ran delete Query')
                    cur.executemany(query, idsToDelete)
                    dblocal.commit()
                except:
                    traceback.print_exc()
                    print('Error in deleting duplicate IDS')
                    exit(1)
            '''        
            with closing( dblocal.cursor() ) as cur: 
                try:
                    query="ALTER TABLE {0} \
                             ADD INDEX msgTimeIndex (MsgTime);".format(table)
                    cur.execute(query)
                    dblocal.commit()   
                except:
                    traceback.print_exc()
                    print('Error in creating msgTimeIndex indexes.')
                    
            with closing( dblocal.cursor() ) as cur: 
                try:
                    query="ALTER TABLE {0} \
                             ADD INDEX prefixIPIndex (MsgTime);".format(table)
                    cur.execute(query)
                    dblocal.commit()   
                except:
                    traceback.print_exc()
                    print('Error in creating prefixIPIndex indexes.')     
            '''
            dblocal.close()
            # print('Indexes Populated')
            return True
        except OperationalError:
            if tryCounter < 3:
                print('No DB Connection available.. Will try again..')
                time.sleep(0.01)
                tryCounter += 1
                pass
            else:
                print('DB connection error.I quit.')
                break
    return False

def loadWorkerForProcessPool(fn):
    tryCounter = 0
    poolWorkerName = current_process().name
    try:
        # print('Got '+fn)
        config = configparser.ConfigParser()
        config.read('./conf/bgpDataEngine.conf')
        config.sections()
        tableDirPath = config['DEFAULTDIRS']['tableDir']
        peer_list = []
        new_peer_list = []

        def localwriteToFile():
            for tableName in toPushData.keys():
                try:
                    resultfile = open(tableDirPath + tableName + '.' + poolWorkerName, 'a')
                    for strg in toPushData[tableName]:
                        print(strg, file=resultfile)
                except Exception as e:
                    print(e)

                finally:
                    resultfile.close()

        def simplfyPath(all_ASes):
            prev = ''
            clean_aspath = []
            for AS in all_ASes:
                if AS != prev:
                    prev = AS
                    clean_aspath.append(AS)
            return clean_aspath

        def localPushData(table, data):
            try:
                dblocal = pymysql.connect(host=config['MySQL']['serverIP'],
                                          port=int(config['MySQL']['serverPort']),
                                          user=config['MySQL']['user'],
                                          passwd=config['MySQL']['password'],
                                          db=config['MySQL']['dbname'])
                with closing(dblocal.cursor()) as cur:

                    ##try:
                    ##    fileName=tableDirPath+table+"."+poolWorkerName
                    ##    query='load data local infile \''+fileName+'\' into table '+table+' fields terminated by \'|\' lines terminated by \'\n\' \
                    ##    (BGPVersion,MsgTime,MsgType,PeerAS,PeerIP,PrefixOriginAS,PrefixIP,PrefixMask,ASPath,ASPathLength,ASPathLengthSimple,Origin,NextHop,LocalPref,Med,Community,AggregateID,AggregateIP,MsgHash) SET ID = NULL;'
                    ##    #print('Pushed '+table)
                    ##    cur.execute(query)
                    ##    dblocal.commit()
                    ##    print('Loaded '+fileName+' to DB',flush=True)
                    ##    os.remove(fileName)
                    ##except:
                    ##    traceback.print_exc()

                    query = "insert ignore into {0} (id,BGPVersion,MsgTime,MsgType,PeerAS,PeerIP,PrefixOriginAS,PrefixIP,PrefixMask,ASPath,ASPathLength,ASPathLengthSimple,Origin,NextHop,LocalPref,Med,Community,AggregateID,AggregateIP,MsgHash)".format(
                        table)
                    #try:
                    cur.executemany(query + " values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", data)
                    dblocal.commit()
                    #except:
                    #    pass

                        #    traceback.print_exc()
                        # for l in data:
                        #    try:
                        #        cur.execute(query+" values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",l)
                        #        dblocal.commit()
                        #    except:
                        # print(table,l)
                        # raise Exception('Insert to table Failed!')
                        #        continue
                        # dblocal.commit()
                        # try:
                        #    query="ALTER TABLE {0} \
                        #                ADD UNIQUE INDEX idx (MsgHash);".format(table)
                        #    cur.execute(query)
                        #    dblocal.commit()
                        # except:
                        #    pass
                #dblocal.commit()
                dblocal.close()

            except OperationalError as exp:
                if tryCounter < 5:
                    # self.logger.error('No DB Connection available.. Will try again..')
                    print('DB connection error. Will try again..',flush=True)
                    time.sleep(1)
                    tryCounter += 1
                    localPushData(table, data)
                else:
                    print('DB connection error. Escaping.',flush=True)
                    return
            except:
                traceback.print_exc()

        def localGetTableName(BGPVersion, MsgTime, PeerIP):
            peerU = PeerIP.replace('.', '_')
            peerU = peerU.replace(':', '_')  # For v6 peers
            day = time.strftime('%Y%m%d', time.gmtime(int(MsgTime)))
            if BGPVersion.startswith("BGP"):
                btype = 'updates'
            elif BGPVersion.startswith('TABLE'):
                btype = 'ribs'
            else:
                return False  # Something funky happened
            table = btype + '_d' + day + '_p' + peerU
            return table

        def localcheckIfTableExists(table):
            dblocal = pymysql.connect(host=config['MySQL']['serverIP'],
                                      port=int(config['MySQL']['serverPort']),
                                      user=config['MySQL']['user'],
                                      passwd=config['MySQL']['password'],
                                      db=config['MySQL']['dbname'])
            with closing(dblocal.cursor()) as cur:
                try:
                    # cur.execute("SHOW TABLES LIKE \'%s\';",table)
                    query = "SHOW TABLES LIKE \'{0}\';".format(table)
                    cur.execute(query)
                    retval = cur.fetchone()
                except:
                    raise Exception('Show tables failed')
            dblocal.close()
            if retval:
                # print('Table '+table+' does not exists')
                return True
            else:
                # print('Table '+table+' does not exists')
                return False

        def localpopulatePeerList():
            dblocal = pymysql.connect(host=config['MySQL']['serverIP'],
                                      port=int(config['MySQL']['serverPort']),
                                      user=config['MySQL']['user'],
                                      passwd=config['MySQL']['password'],
                                      db=config['MySQL']['dbname'])
            with closing(dblocal.cursor()) as cur:
                try:
                    query = "select PeerIP from peer_list;"
                    cur.execute(query)
                    row = cur.fetchone()
                    while row is not None:
                        if row not in peer_list:
                            peer_list.append(row)
                        row = cur.fetchone()
                except:
                    raise Exception('Select to table Failed!')
            dblocal.close()

        def localpushPeerIP():
            dblocal = pymysql.connect(host=config['MySQL']['serverIP'],
                                      port=int(config['MySQL']['serverPort']),
                                      user=config['MySQL']['user'],
                                      passwd=config['MySQL']['password'],
                                      db=config['MySQL']['dbname'])
            with closing(dblocal.cursor()) as cur:
                for PeerIP in new_peer_list:
                    try:
                        query = "insert into peer_list (PeerIP) values (\'{0}\');".format(PeerIP)
                        cur.execute(query)
                        dblocal.commit()
                    except:
                        # Can give dupicate entry error, ignore
                        # traceback.print_exc()
                        # exit(0)
                        pass
            dblocal.close()

        def localPopulatePeerDayTable(table):
            Tabletype = table.split('_d')[0]
            peer = table.split('_p')[1].replace('_', '.')
            day = table.split('_d')[1].split('_p')[0]
            isPresent = False
            dblocal = pymysql.connect(host=config['MySQL']['serverIP'],
                                      port=int(config['MySQL']['serverPort']),
                                      user=config['MySQL']['user'],
                                      passwd=config['MySQL']['password'],
                                      db=config['MySQL']['dbname'])
            with closing(dblocal.cursor()) as cur:
                try:
                    query = "select * from peer_day where PeerIP = \'{0}\' and Day = \'{1}\';".format(peer, day)
                    cur.execute(query)
                    row = cur.fetchone()
                    if row:
                        isPresent = True
                except:
                    raise Exception('Select to peer_day table Failed!')

            if isPresent:
                # print('Row is present')
                if Tabletype == 'updates':
                    Ttype = 'Updates'
                else:
                    Ttype = 'Ribs'
                with closing(dblocal.cursor()) as cur:
                    try:
                        query = "UPDATE peer_day SET {0}=1 WHERE PeerIP=\'{1}\' and Day=\'{2}\';".format(Ttype,
                                                                                                         peer, day)
                        cur.execute(query)
                        dblocal.commit()
                    except:
                        # Some other thread must have already created it
                        dblocal.close()
            else:
                # print('Row is not present')
                with closing(dblocal.cursor()) as cur:
                    # print('Table type: '+Tabletype)
                    try:
                        if Tabletype == 'updates':
                            # print('Table type: '+Tabletype)
                            query = "insert into peer_day (PeerIP,Day,Updates,Ribs) values (\'{0}\',\'{1}\',\'{2}\',\'{3}\');".format(
                                peer, day, 1, 0)
                            # print('inserted for updates table')
                        else:
                            query = "insert into peer_day (PeerIP,Day,Updates,Ribs) values (\'{0}\',\'{1}\',\'{2}\',\'{3}\');".format(
                                peer, day, 0, 1)

                        cur.execute(query)
                        dblocal.commit()
                    except:
                        print('Update Error for peer_day!!')
                        traceback.print_exc()
                        exit(1)

            dblocal.close()

        def localcreateTable(table):
            dblocal = pymysql.connect(host=config['MySQL']['serverIP'],
                                      port=int(config['MySQL']['serverPort']),
                                      user=config['MySQL']['user'],
                                      passwd=config['MySQL']['password'],
                                      db=config['MySQL']['dbname'])
            with closing(dblocal.cursor()) as cur:
                try:
                    if table.startswith('updates') or table.startswith('ribs'):
                        query = "CREATE TABLE IF NOT EXISTS {0} ( \
                                    ID BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,\
                                    BGPVersion VARCHAR(20) NOT NULL,\
                                    MsgTime INT UNSIGNED NOT NULL,\
                                    MsgType VARCHAR(1) NOT NULL,\
                                    PeerAS INT UNSIGNED NOT NULL,\
                                    PeerIP VARCHAR(255) NOT NULL,\
                                    PrefixOriginAS INT UNSIGNED,\
                                    PrefixIP VARCHAR(255) NOT NULL,\
                                    PrefixMask INT UNSIGNED NOT NULL,\
                                    ASPath VARCHAR(999),\
                                    ASPathLength INT UNSIGNED,\
                                    ASPathLengthSimple INT UNSIGNED,\
                                    Origin VARCHAR(20),\
                                    NextHop VARCHAR(255) NOT NULL,\
                                    LocalPref INT UNSIGNED,\
                                    Med INT UNSIGNED,\
                                    Community VARCHAR(255),\
                                    AggregateID VARCHAR(255),\
                                    AggregateIP VARCHAR(255),  \
                                    MsgHash VARCHAR(255),  \
                                    INDEX msgTimeIndex (MsgTime),\
                                    INDEX prefixIPIndex (PrefixIP), \
                                    PRIMARY KEY (ID));".format(table)
                    else:
                        return False  # Something does not look right with table name

                    cur.execute(query)
                    dblocal.commit()
                    # query="ALTER TABLE {0} \
                    #        ADD UNIQUE INDEX idx (MsgHash);".format(table)
                    # cur.execute(query)
                    # dblocal.commit()
                    # self.logger.info('Created '+table+' table.')
                    # print('Created '+table+' table.')
                    dblocal.close()
                    localPopulatePeerDayTable(table)
                    return True
                except:
                    # traceback.print_exc()
                    # raise Exception('Create table failed!')
                    #self.logger.warn('Table ' + table + ' creation did not go well.')
                    # exit(1)
                    return False

        toPushData = {}
        lines = []
        localpopulatePeerList()

        try:
            lines = subprocess.check_output(["bgpdump", "-m", fn], universal_newlines=True)
        except:
            # self.logger.warn('BGP file '+fn+' could not be read properly. Skipping it.')
            print('BGP file could not be read properly',flush=True)
            return []

        # self.logger.info('Reading entries for '+fn)
        # print('Looping through lines')
        counter = 0
        for line in lines.split("\n"):
            line.rstrip('\n')
            if line.startswith("BGP") or line.startswith('TABLE'):  # Eliminate possibility of empty rows
                pieces = line.split('|')
                # print(pieces)
                (BGPVersion, MsgTime, MsgType, PeerIP, PeerAS, PrefixCom, ASPath, Origin, NextHop, LocalPref, Med,
                 Community, AggregateID, AggregateIP) = [""] * 14
                if len(pieces) == 15:
                    (BGPVersion, MsgTime, MsgType, PeerIP, PeerAS, PrefixCom, ASPath, Origin, NextHop, LocalPref,
                     Med, Community, AggregateID, AggregateIP, _) = pieces
                else:
                    (BGPVersion, MsgTime, MsgType, PeerIP, PeerAS, PrefixCom) = pieces
                PrefixFields = PrefixCom.split('/')
                PrefixIP = PrefixFields[0]
                PrefixMask = PrefixFields[1]

                # Check if valid Peer
                v4PeerFlag = False
                v6PeerFlag = False
                try:
                    if ipaddress.IPv4Address(PeerIP):
                        v4PeerFlag = True  # Valid v4 peer
                except:
                    try:
                        if ipaddress.IPv6Address(PeerIP):
                            v6PeerFlag = True  # Valid v6 peer and prefix
                    except:
                        print("Saw invalid peer IP: " + PeerIP)

                if not v4PeerFlag and not v6PeerFlag:
                    continue  # No valid peer

                # Check if valid Prefix
                v4PrefixFlag = False
                v6PrefixFlag = False
                try:
                    if ipaddress.IPv4Network(PrefixIP):
                        v4PrefixFlag = True  # Valid v4 prefix
                except:
                    try:
                        if ipaddress.IPv6Network(PrefixIP):
                            v6PrefixFlag = True  # Valid v6 prefix
                    except:
                        print("Saw invalid prefix: " + PrefixIP + '/' + PrefixMask)

                if not v4PrefixFlag and not v6PrefixFlag:
                    continue  # No valid prefix

                # intPrefixIP=int(netaddr.IPAddress(PrefixIP))
                # Not keep IPs as ints is a concious choice because of bad int16 supports
                # Plus MySQL 5.5 has not inet6

                if v6PeerFlag:
                    PeerIP = ipaddress.IPv6Address(PeerIP).exploded  # Full representation
                if v6PrefixFlag:
                    PrefixIP = ipaddress.IPv6Address(PrefixIP).exploded  # Full representation

                if PeerIP not in peer_list:
                    if PeerIP not in new_peer_list:
                        new_peer_list.append(PeerIP)

                ASPathList = ASPath.split(' ')
                lenASPath = len(ASPathList)
                if ASPath == "":
                    lenASPath = ""
                    lenASPathClean = ""
                    PrefixOriginAS = ""
                else:
                    lenASPathClean = len(simplfyPath(ASPathList))
                    PrefixOriginAS = ASPathList[lenASPath - 1]

                tableName = localGetTableName(BGPVersion, MsgTime, PeerIP)
                if not tableName:
                    continue  # Skip this message

                if tableName not in toPushData.keys():
                    toPushData[tableName] = []

                # str(counter)+'|'+
                strg = BGPVersion + '|' + MsgTime + '|' + MsgType + '|' + str(PeerAS) + '|' + str(
                    PeerIP) + '|' + str(PrefixOriginAS) + '|' + str(PrefixIP) + '|' + str(
                    PrefixMask) + '|' + ASPath + '|' + str(lenASPath) + '|' + str(
                    lenASPathClean) + '|' + Origin + '|' + str(NextHop) + '|' + str(LocalPref) + '|' + str(
                    Med) + '|' + Community + '|' + AggregateID + '|' + AggregateIP
                MsgHash = hashlib.md5(strg.encode('utf-8')).hexdigest()
                strg = strg + '|' + MsgHash
                toPushData[tableName].append(strg)
                counter += 1

        localpushPeerIP()
        # Create required tables
        for tableName in toPushData.keys():
            if not localcheckIfTableExists(tableName):
                # Table does not exist. Create it.
                if not localcreateTable(tableName):
                    # print('There was create table error for '+tableName)
                    continue  # No table was created will skip

        # Write to local files
        #localwriteToFile()
        pushToDB = True
        # Push to DB
        if pushToDB:
            for table in toPushData.keys():
                data = []
                for line in toPushData[table]:
                    rline = line.split('|')
                    datatmp = []
                    datatmp.append('None')
                    for v in rline:
                        datatmp.append(v)
                    data.append(datatmp)
                print('{0} loading data..'.format(poolWorkerName),flush=True)
                localPushData(table, data)
                print('{0} finished loading data.'.format(poolWorkerName),flush=True)
        tableList = list(toPushData.keys())
        if tableList:
            return tableList  # Tables that were updated
        else:
            return []

    except:
        traceback.print_exc()
    print('Returning Empty List 1',flush=True)
    return []
