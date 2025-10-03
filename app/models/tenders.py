from typing import List, Optional
from sqlalchemy import Text, ForeignKey, Column, Integer, Float, DateTime, func, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class TenderInfo(Base):
    __tablename__ = 'tenders_info'

    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[Optional[str]] = mapped_column(UUID)
    user_id: Mapped[Optional[str]] = mapped_column(UUID)
    external_tender_id: Mapped[Optional[str]] = mapped_column(UUID)
    tender_name: Mapped[Optional[str]] = mapped_column(Text)
    tender_number: Mapped[Optional[str]] = mapped_column(Text)
    customer_name: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)
    purchase_type: Mapped[Optional[str]] = mapped_column(Text)
    financing_source: Mapped[Optional[str]] = mapped_column(Text)
    amount: Mapped[Optional[int]]
    currency: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(),
                                                 onupdate=func.now())
    processed_positions: Mapped[Optional[int]] = mapped_column(server_default='0', nullable=False, default=0)

    # Relationships
    delivery_info: Mapped[List["DeliveryInfo"]] = relationship(
        back_populates="tender",
        cascade="all, delete-orphan"
    )
    payment_info: Mapped[List["PaymentInfo"]] = relationship(
        back_populates="tender",
        cascade="all, delete-orphan"
    )
    tender_positions: Mapped[List["TenderPositions"]] = relationship(
        back_populates="tender",
        cascade="all, delete-orphan"
    )
    general_requirements: Mapped[List["GeneralRequirements"]] = relationship(
        back_populates="tender",
        cascade="all, delete-orphan"
    )
    attachments: Mapped[List["Attachments"]] = relationship(
        back_populates="tender",
        cascade="all, delete-orphan"
    )


class DeliveryInfo(Base):
    __tablename__ = 'tenders_delivery_info'

    id: Mapped[int] = mapped_column(primary_key=True)
    tender_id: Mapped[int] = mapped_column(ForeignKey('tenders_info.id'))
    address: Mapped[Optional[str]] = mapped_column(Text)
    term: Mapped[Optional[str]] = mapped_column(Text)
    conditions: Mapped[Optional[str]] = mapped_column(Text)

    # Relationship
    tender: Mapped["TenderInfo"] = relationship(back_populates="delivery_info")


class PaymentInfo(Base):
    __tablename__ = 'tenders_payment_info'

    id: Mapped[int] = mapped_column(primary_key=True)
    tender_id: Mapped[int] = mapped_column(ForeignKey('tenders_info.id'))
    term: Mapped[Optional[str]] = mapped_column(Text)
    method: Mapped[Optional[str]] = mapped_column(Text)
    conditions: Mapped[Optional[str]] = mapped_column(Text)

    # Relationship
    tender: Mapped["TenderInfo"] = relationship(back_populates="payment_info")


class TenderPositions(Base):
    __tablename__ = 'tenders_positions'

    id: Mapped[int] = mapped_column(primary_key=True)
    tender_id: Mapped[int] = mapped_column(ForeignKey('tenders_info.id'))
    tender_position: Mapped[Optional[int]]
    title: Mapped[Optional[str]] = mapped_column(Text)
    okpd2_code: Mapped[Optional[str]] = mapped_column(Text)
    ktru_code: Mapped[Optional[str]] = mapped_column(Text)
    quantity: Mapped[Optional[int]]
    unit_of_measurement: Mapped[Optional[str]] = mapped_column(Text)
    unit_price: Mapped[Optional[int]]
    total_price: Mapped[Optional[int]]
    currency: Mapped[Optional[str]] = mapped_column(Text)
    additional_requirements: Mapped[Optional[str]] = mapped_column(Text)
    category: Mapped[Optional[str]] = mapped_column(Text)
    category_id: Mapped[Optional[int]]

    # Relationships
    tender: Mapped["TenderInfo"] = relationship(back_populates="tender_positions")
    attributes: Mapped[List["TenderPositionAttributes"]] = relationship(
        back_populates="tender_position",
        cascade="all, delete-orphan"
    )

class TenderPositionAttributesMatches(Base):
    __tablename__ = 'tenders_position_attributes_matches'

    id: Mapped[int] = mapped_column(primary_key=True)
    tender_id: Mapped[Optional[int]]
    tender_position_id: Mapped[int] = mapped_column(ForeignKey('tenders_positions.id'))
    position_attr_id: Mapped[Optional[int]]
    product_mongo_id: Mapped[Optional[int]]
    position_attr_name: Mapped[Optional[str]] = mapped_column(Text)
    position_attr_value: Mapped[Optional[str]] = mapped_column(Text)
    position_attr_unit: Mapped[Optional[str]] = mapped_column(Text)
    product_attr_name: Mapped[Optional[str]] = mapped_column(Text)
    product_attr_value: Mapped[Optional[str]] = mapped_column(Text)

class TenderPositionAttributes(Base):
    __tablename__ = 'tenders_position_attributes'

    id: Mapped[int] = mapped_column(primary_key=True)
    tender_position_id: Mapped[int] = mapped_column(ForeignKey('tenders_positions.id'))
    characteristic_id: Mapped[Optional[int]]
    name: Mapped[Optional[str]] = mapped_column(Text)
    value: Mapped[Optional[str]] = mapped_column(Text)
    unit: Mapped[Optional[str]] = mapped_column(Text)
    required: Mapped[Optional[bool]]
    changeable: Mapped[Optional[bool]]
    fill_instructions: Mapped[Optional[str]] = mapped_column(Text)
    type: Mapped[Optional[str]] = mapped_column(Text)

    # Relationship
    tender_position: Mapped["TenderPositions"] = relationship(back_populates="attributes")


class GeneralRequirements(Base):
    __tablename__ = 'tenders_general_requirements'

    id: Mapped[int] = mapped_column(primary_key=True)
    tender_id: Mapped[int] = mapped_column(ForeignKey('tenders_info.id'))
    quality_requirements: Mapped[Optional[str]] = mapped_column(Text)
    packaging_requirements: Mapped[Optional[str]] = mapped_column(Text)
    marking_requirements: Mapped[Optional[str]] = mapped_column(Text)
    warranty_requirements: Mapped[Optional[str]] = mapped_column(Text)
    safety_requirements: Mapped[Optional[str]] = mapped_column(Text)
    regulatory_requirements: Mapped[Optional[str]] = mapped_column(Text)

    # Relationship
    tender: Mapped["TenderInfo"] = relationship(back_populates="general_requirements")


class Attachments(Base):
    __tablename__ = 'tenders_attachments'

    id: Mapped[int] = mapped_column(primary_key=True)
    tender_id: Mapped[int] = mapped_column(ForeignKey('tenders_info.id'))
    name: Mapped[Optional[str]] = mapped_column(Text)
    type: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)
    url: Mapped[Optional[str]] = mapped_column(Text)

    # Relationship
    tender: Mapped["TenderInfo"] = relationship(back_populates="attachments")


class Matches(Base):
    __tablename__ = 'tender_matches'

    id = Column(Integer, primary_key=True, autoincrement=True)
    tender_position_id = Column(Integer, ForeignKey('tenders_positions.id'))
    product_id = Column(Integer)
    match_score = Column(Integer)
    max_match_score = Column(Integer)
    percentage_match_score = Column(Float)
