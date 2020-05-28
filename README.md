Minimalistic high-volume multi-threaded archive-tool for imagegallery sites.

Currently Supports:
 - Danbooru
 - Gelbooru
 - http://rule34.xxx/
 - https://e621.net/
 - https://konachan.com/
 - https://tbib.org/
 - https://xbooru.com/

Written because I needed a extremely large image database *with tags* to 
use for some experiments with training neural nets.

Requires:
 - Sqlalchemy
 - A Database of some sort (currently only works with postgres)
 - Beautiful Soup 4
 - Python 3

Potential ideas:
 - Add support for:
	 - http://www.zerochan.net/
	 - http://e-shuushuu.net/
	 - https://chan.sankakucomplex.com/
	 - http://paheal.net/

Note: There is a pre-available Danbooru-specific archive available in various formats here: https://www.gwern.net/Danbooru2019
This may be relevant if you're just looking for a easily-available dataset.
