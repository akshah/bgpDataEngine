'''
Created on Oct 9, 2015

@author: akshah
'''
from bgpDataEngine.bgpDataEngine import bgpDataEngine
from customUtilities.helperFunctions import *


if __name__ == '__main__':
    start_time,_=currentTime()

    bde=bgpDataEngine()

    #Fetch entire month
    #bde.getMonth('updates','2016','03',load2db=False,collectors=['route-views.jinx','rrc00','bgpmon'])

    accessToBGPMonArchive = True
    self.accessToRVArchive = False
    self.accessToRipeArchive = False

    bde.getRange('updates','20140701','20140705',collectors=['routeviews-kixp'])

    end_time,_=currentTime()
    print('Finished processing in '+str(int((end_time-start_time)/60))+' minutes and '+str(int((end_time-start_time)%60))+' seconds.')
