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
    bde.getMonth('Updates','2014','03')


    #bde.getRange('updates','20140701','20140705',collectors=['route-views.jinx'])

    Getting data from DB
    day='20140705'
    peer='196.223.14.10'
    day='20140701'
    #peer='195_66_224_114'
    bde.getUpdateMessages(peer,day)
    while True:
        val=bde.messageQueue.get()
        if not val:
            break
        print(val)

    end_time,_=currentTime()
    print('Finished processing in '+str(int((end_time-start_time)/60))+' minutes and '+str(int((end_time-start_time)%60))+' seconds.')
