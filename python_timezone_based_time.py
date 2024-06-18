import pytz
from datetime import datetime
nz_tz = pytz.timezone('Pacific/Auckland')
current_time = datetime.now(nz_tz)
file_creation_date=current_time.strftime('%Y%m%d')
file_creation_time=current_time.strftime("%H%M%S")
