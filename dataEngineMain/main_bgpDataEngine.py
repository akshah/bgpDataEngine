'''
Created on Oct 9, 2015

@author: akshah
'''
from bgpDataEngine.bgpDataEngine import bgpDataEngine
from customUtilities.helperFunctions import *


if __name__ == '__main__':
    start_time,_=current_time()
    
    bde=bgpDataEngine()
    
    #Fetch entire month
    #bde.getMonth('Updates','2014','03')

        
    #start='20150130000000'
    #end='20150131000100'
    #bde.getRangeFromBGPMon('updates',start,end)

    #Just load files no fetching
    #mrtFiles = [join('mrtFiles/', f) for f in listdir('mrtFiles/') if isfile(join('mrtFiles/', f)) if join('mrtFiles/',f)[-4:]=='.mrt'] 
    #bde.load2DB(mrtFiles)
    
    
    #bde.downloadFile()
    bde.getRange('updates','20140701','20140705',collectors=['route-views.jinx']) 
    
    #bde.load(['/raid1/akshah/mrtFiles/route-views.kixp/2015/11/01/route-views.kixp.updates.20151101.0000.bz2','/raid1/akshah/mrtFiles/route-views.kixp/2015/11/01/route-views.kixp.updates.20151101.1200.bz2'])
    #bde.load2DB()
    
    #Getting data from DB
    #day='20140705'
    #peer='196_223_14_10'
    #day='20140701'
    #peer='195_66_224_114'
    #bde.getUpdateMessages(peer,day)
    #while True:
    #    val=bde.messageQueue.get()
    #    if not val:
    #        break
    #    print(val)
    
    end_time,_=current_time()
    print('Finished processing in '+str(int((end_time-start_time)/60))+' minutes and '+str(int((end_time-start_time)%60))+' seconds.')
