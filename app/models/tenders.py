from typing import List, Optional
from sqlalchemy import Text, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class TenderInfo(Base):
    __tablename__ = 'tenders_info'

    id: Mapped[int] = mapped_column(primary_key=True)
    tender_name: Mapped[Optional[str]] = mapped_column(Text)
    tender_number: Mapped[Optional[str]] = mapped_column(Text)
    customer_name: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)
    purchase_type: Mapped[Optional[str]] = mapped_column(Text)
    financing_source: Mapped[Optional[str]] = mapped_column(Text)
    amount: Mapped[Optional[int]]
    currency: Mapped[Optional[str]] = mapped_column(Text)

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