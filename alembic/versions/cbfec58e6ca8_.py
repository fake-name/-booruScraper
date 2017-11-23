"""empty message

Revision ID: cbfec58e6ca8
Revises: 
Create Date: 2017-11-23 05:24:08.420515

"""

# revision identifiers, used by Alembic.
revision = 'cbfec58e6ca8'
down_revision = None
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


# Patch in knowledge of the citext type, so it reflects properly.
from sqlalchemy.dialects.postgresql.base import ischema_names
import citext
import queue
import datetime
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.dialects.postgresql import TSVECTOR
ischema_names['citext'] = citext.CIText



def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_db_releases_source_dlstate_postid', table_name='db_releases')
    op.drop_column('db_releases', 'filepath')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('db_releases', sa.Column('filepath', sa.TEXT(), autoincrement=False, nullable=True))
    op.create_index('ix_db_releases_source_dlstate_postid', 'db_releases', ['source', 'dlstate', 'postid'], unique=False)
    # ### end Alembic commands ###
