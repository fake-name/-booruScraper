
import settings

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy import Table
from sqlalchemy import Index

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import BigInteger
from sqlalchemy import Text
from sqlalchemy import text
from sqlalchemy import Float
from sqlalchemy import Boolean
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy

# Patch in knowledge of the citext type, so it reflects properly.
from sqlalchemy.dialects.postgresql.base import ischema_names
from sqlalchemy.dialects.postgresql import ENUM

import citext
ischema_names['citext'] = citext.CIText
dlstate_enum   = ENUM('new', 'fetching', 'processing', 'complete', 'error', 'removed', 'disabled', name='dlstate_enum')


from settings import DATABASE_IP            as C_DATABASE_IP
from settings import DATABASE_DB_NAME       as C_DATABASE_DB_NAME
from settings import DATABASE_USER          as C_DATABASE_USER
from settings import DATABASE_PASS          as C_DATABASE_PASS

SQLALCHEMY_DATABASE_URI = 'postgresql://{user}:{passwd}@{host}:5432/{database}'.format(user=C_DATABASE_USER, passwd=C_DATABASE_PASS, host=C_DATABASE_IP, database=C_DATABASE_DB_NAME)

DB_CONNECTION_POOL_SIZE = 20

# I was having issues with timeouts because the default connection pool is 5 connections.
engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size = DB_CONNECTION_POOL_SIZE, isolation_level='REPEATABLE_READ')

SessionFactory = sessionmaker(bind=engine)
session = scoped_session(SessionFactory)
# session = Session()
Base = declarative_base()

db_tags_link = Table(
		'db_tags_link', Base.metadata,
		Column('releases_id', Integer, ForeignKey('db_releases.id'), nullable=False),
		Column('tags_id',     Integer, ForeignKey('db_tags.id'),     nullable=False),
		PrimaryKeyConstraint('releases_id', 'tags_id')
	)

db_chars_link = Table(
		'db_chars_link', Base.metadata,
		Column('releases_id', Integer, ForeignKey('db_releases.id'), nullable=False),
		Column('character_id', Integer, ForeignKey('db_characters.id'), nullable=False),
		PrimaryKeyConstraint('releases_id', 'character_id')
	)

db_artist_link = Table(
		'db_artist_link', Base.metadata,
		Column('releases_id', Integer, ForeignKey('db_releases.id'), nullable=False),
		Column('type_id',     Integer, ForeignKey('db_artist.id'),     nullable=False),
		PrimaryKeyConstraint('releases_id', 'type_id')
	)

db_file_link = Table(
		'db_file_link', Base.metadata,
		Column('releases_id', Integer, ForeignKey('db_releases.id'), nullable=False),
		Column('file_id',     Integer, ForeignKey('db_files.id'),     nullable=False),
		PrimaryKeyConstraint('releases_id', 'file_id')
	)

class RawPages(Base):
	__tablename__ = 'db_raw_pages'
	id          = Column(Integer, primary_key = True)
	dlstate     = Column(Integer, default=0, index = True)
	sourceurl   = Column(Text,    nullable = False, index = True)
	pgctnt      = Column(Text)
	scantime    = Column(DateTime)
	urltype     = Column(Integer, nullable = False)



class Tags(Base):
	__tablename__ = 'db_tags'
	id          = Column(Integer, primary_key=True)
	tag         = Column(citext.CIText(), nullable=False, index=True)

	__table_args__ = (
		UniqueConstraint('tag'),
		)

class Characters(Base):
	__tablename__ = 'db_characters'
	id          = Column(Integer, primary_key=True)
	character   = Column(citext.CIText(), nullable=False, index=True)

	__table_args__ = (
		UniqueConstraint('character'),
		)

class Artist(Base):
	__tablename__ = 'db_artist'
	id          = Column(Integer, primary_key=True)
	artist      = Column(citext.CIText(), nullable=False, index=True)

	__table_args__ = (
		UniqueConstraint('artist'),
		)


class Files(Base):
	__tablename__ = 'db_files'
	id          = Column(BigInteger, primary_key=True)

	filepath    = Column(citext.CIText(), nullable=False)
	fhash       = Column(Text, nullable=False)

	phash       = Column(BigInteger)
	imgx        = Column(Integer)
	imgy        = Column(Integer)

	__table_args__ = (
		UniqueConstraint('filepath'),
		UniqueConstraint('fhash'),
		Index('phash_bktree_idx', 'phash', postgresql_using="spgist")
		)

def tag_creator(tag):

	tmp = session.query(Tags)         \
		.filter(Tags.tag      == tag) \
		.scalar()
	if tmp:
		return tmp

	return Tags(tag=tag)

def character_creator(char):
	tmp = session.query(Characters)           \
		.filter(Characters.character == char) \
		.scalar()
	if tmp:
		return tmp
	return Characters(character=char)

def artist_creator(artist):
	tmp = session.query(Artist)                  \
		.filter(Artist.artist == artist) \
		.scalar()
	if tmp:
		return tmp
	return Artist(artist=artist)


def file_creator(filetups):
	filepath, fhash = filetups

	# We only care about uniqueness WRT hashes.
	tmp = session.query(Files)         \
		.filter(Files.fhash  == fhash) \
		.scalar()
	if tmp:
		return tmp

	# Remove the absolute path (if needed)
	if settings.storeDir in filepath:
		filepath = filepath[len(settings.storeDir):]
	return Files(filepath=filepath, fhash=fhash)


class Releases(Base):
	__tablename__ = 'db_releases'
	id          = Column(Integer, primary_key=True)
	state       = Column(dlstate_enum, nullable=False, index=True, default='new')
	err_str     = Column(Text)
	postid      = Column(Integer, nullable=False, index=True)

	source      = Column(citext.CIText, nullable=False, index=True)

	fsize       = Column(BigInteger)
	score       = Column(Float)
	favourites  = Column(Integer)

	parent      = Column(Text)

	posted      = Column(DateTime)

	res_x       = Column(Integer)
	res_y       = Column(Integer)


	status      = Column(Text)
	rating      = Column(Text)


	tags_rel      = relationship('Tags',       secondary=lambda: db_tags_link)
	character_rel = relationship('Characters', secondary=lambda: db_chars_link)
	artist_rel    = relationship('Artist',     secondary=lambda: db_artist_link)
	file_rel      = relationship('Files',      secondary=lambda: db_file_link)

	tags          = association_proxy('tags_rel',      'tag',       creator=tag_creator)
	character     = association_proxy('character_rel', 'character', creator=character_creator)
	artist        = association_proxy('artist_rel',    'artist',    creator=artist_creator)
	file          = association_proxy('file_rel',      'files',    creator=file_creator)

	__table_args__ = (
		UniqueConstraint('postid', 'source'),
		Index('db_releases_source_state_id_idx', 'source', 'state', 'id')
		)



Base.metadata.create_all(bind=engine, checkfirst=True)



