'''
Created on Mar 10, 2016

@author: akshah
'''
from bgpDataEngine.bgpDataEngine import bgpDataEngine
from customUtilities.helperFunctions import *


if __name__ == '__main__':
    start_time,_=currentTime()

    bde=bgpDataEngine(configfile='conf/custom.conf')
    #Simply read an MRT file
    file="rrc00.bview.20160301.0000.gz"
    bde.readMRT(file)
    while True:
        val=bde.messageQueue.get()
        if not val:
            break
        print(val)

    end_time,_=currentTime()
    print('Finished processing in '+str(int((end_time-start_time)/60))+' minutes and '+str(int((end_time-start_time)%60))+' seconds.')
