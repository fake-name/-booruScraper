
# Your postgres SQL database credentials for the primary database.
# the DATABASE_USER must have write access to the database DATABASE_DB_NAME
DATABASE_USER    = "dbarchiver"
DATABASE_PASS    = "YEkTYt4sCcWctY"
DATABASE_DB_NAME = "dbmirror"
DATABASE_IP      = "10.1.1.8"
# Note that a local socket will be tried before the DATABASE_IP value, so if DATABASE_IP is
# invalid, it may work anyways.


# The directory "Context" of all the hentai items.
# This determines the path mask that will be used when deduplicating
# hentai items.
# If you aren't running the deduper, just specify something basic, like "/"
storeDir            = r"/media/Storage/H/Danbooru"


