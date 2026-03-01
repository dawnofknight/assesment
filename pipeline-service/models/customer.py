from sqlalchemy import Column, String, Date, Numeric, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from database import Base


class Customer(Base):
    __tablename__ = "customers"

    customer_id = Column(String(50), primary_key=True, index=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    email = Column(String(255), nullable=False)
    phone = Column(String(20), nullable=True)
    address = Column(Text, nullable=True)
    date_of_birth = Column(Date, nullable=True)
    account_balance = Column(Numeric(15, 2), nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), nullable=True)
