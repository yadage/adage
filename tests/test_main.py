import adage
import logging
import time
def test_simeexample():

    adageobj = adage.adageobject()
    adage.rundag(adageobj, default_trackers = False)
