'''
Created on Oct 9, 2015

@author: akshah
'''
from bgpDataEngine.bgpDataEngine import bgpDataEngine
from customUtilities.helperFunctions import *


if __name__ == '__main__':
    start_time,_=currentTime()

    bde=bgpDataEngine()

    #Just load files no fetching
    mrtFiles = [join('mrtFiles/', f) for f in listdir('mrtFiles/') if isfile(join('mrtFiles/', f)) if join('mrtFiles/',f)[-4:]=='.mrt']
    bde.load2DB(mrtFiles)

    end_time,_=currentTime()
    print('Finished processing in '+str(int((end_time-start_time)/60))+' minutes and '+str(int((end_time-start_time)%60))+' seconds.')
