from database import Data
from helper import Helper

def test():
    Helper.printline("** Start")
    d = Data()
    views = d.get_views()
    d.delete_views(views)
    Helper.printline("Deleted all views")
    d.create_vehicle_mainenance_view()
    Helper.printline("** Ended")
    
if __name__ == "__main__":
    test()