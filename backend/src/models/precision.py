from sqlalchemy import Column,Integer,String,DateTime,func
from src.config.database import Base
class PrecisionState(Base):
 __tablename__='precision_state'
 id=Column(Integer,primary_key=True)
 phase=Column(String(20),nullable=False,default='legacy')
 version=Column(Integer,nullable=False,default=0)
 target_id=Column(Integer,nullable=True)
 cursor_id=Column(Integer,nullable=False,default=0)
 total_rows=Column(Integer,nullable=False,default=0)
 backfilled_rows=Column(Integer,nullable=False,default=0)
 reconcile_total=Column(Integer,nullable=False,default=0)
 reconcile_mismatch=Column(Integer,nullable=False,default=0)
 reconcile_checksum=Column(String(64),nullable=True)
 reconcile_at=Column(DateTime,nullable=True)
 lossy_rows=Column(Integer,nullable=True)
 message=Column(String(500),nullable=False,default='')
 updated_at=Column(DateTime,server_default=func.now(),onupdate=func.now())
