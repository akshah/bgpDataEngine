'''
Created on Mar 10, 2016

@author: akshah
'''
from bgpDataEngine.bgpDataEngine import bgpDataEngine
from customUtilities.helperFunctions import *


if __name__ == '__main__':
    start_time,_=currentTime()

    bde=bgpDataEngine()
    #Simply read an MRT file
    file="rrc00.bview.20160301.0000.gz"

    bde.readMRT(file)
    try:
        val=bde.messageQueue.get()
        while val:
            val=bde.messageQueue.get()
    except:
        print('Error in reading message queue.')

    #if not val:
    #    break
    #print(val)

    end_time,_=currentTime()
    print('Finished processing in '+str(int((end_time-start_time)/60))+' minutes and '+str(int((end_time-start_time)%60))+' seconds.')
