from GPSPhoto import gpsphoto
from datetime import dateti me


def decdeg2dms(dd):
    mnt, sec = divmod(dd * 3600, 60)
    deg, mnt = divmod(mnt, 60)
    return deg, mnt, sec


img = 'palm-tree-1.jpg'
data = gpsphoto.getGPSData(img)
lat_deg, lat_mnt, lat_sec = decdeg2dms(data['Latitude'])
lon_deg, lon_mnt, lon_sec = decdeg2dms(data['Longitude'])
mm, dd, yyyy = data['Date'].split('/')
time = data['UTC-Time'].split('.')[0]
hours, mins, secs = time.split(':')
if len(hours) < 2:
    hours = '0' + hours
if len(mins) < 2:
    mins = '0' + mins
if len(secs) < 2:
    secs = '0' + secs
new_time = hours + ':' + mins + ':' + secs
iso_dt = yyyy + '-' + mm + '-' + dd + ' ' + new_time
# date_string = "2008-10-23 14:27:07"
dt = datetime.fromisoformat(iso_dt)
print('c')
