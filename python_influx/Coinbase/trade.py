import decimal

TRADE_ID = "trade_id"
SIDE = "side"
SIZE_SCALE = "size_scale"
SIZE_UNSCALED = "size_unscaled"
PRICE_SCALE = "price_scale"
PRICE_UNSCALED = "price_unscaled"
TIME = "time"


class Trade:
    def __init__(self, trade_id, side, size_scale, size_unscaled, price_scale, price_unscaled, time):
        self.trade_id = trade_id
        self.side = side
        self.size_scale = size_scale
        self.size_unscaled = size_unscaled
        self.price_scale = price_scale
        self.price_unscaled = price_unscaled
        self.time = time

    @classmethod
    def from_trade_id(cls, loader, trade_id):
        return loader.get_trade_id(trade_id)

    @classmethod
    def from_attributes(cls,
                        trade_id,
                        side,
                        size,
                        price,
                        time):
        size_int, size_scale = cls.split_float_str(size)
        price_int, price_scale = cls.split_float_str(price)
        return cls(
            trade_id,
            side,
            size_scale,
            size_int,
            price_scale,
            price_int,
            time
        )

    @classmethod
    def from_dict(cls, entry):
        return cls(
            entry[TRADE_ID],
            entry[SIDE],
            entry[SIZE_SCALE],
            entry[SIZE_UNSCALED],
            entry[PRICE_SCALE],
            entry[PRICE_UNSCALED],
            entry[TIME]
        )

    @classmethod
    def split_float_str(cls, s):
        dec = decimal.Decimal(s)
        t = dec.as_tuple()
        i = 0
        for d in t[1]:
            i = i * 10 + d
        return i, t[2]

    @property
    def price(self):
        return self.price_unscaled * 10 ** self.price_scale

    def price_decimal(self):
        return self._to_decimal(self.price_unscaled, self.price_scale)

    @property
    def size(self):
        return self.size_unscaled * 10 ** self.size_scale

    def size_decimal(self):
        return self._to_decimal(self.size_unscaled, self.size_scale)

    @property
    def volume(self):
        return self.price * self.size

    def volume_decimal(self):
        return self.size_decimal() * self.price_decimal()

    def to_dict(self):
        return {
            TRADE_ID: self.trade_id,
            SIDE: self.side,
            SIZE_SCALE: self.size_scale,
            SIZE_UNSCALED: self.size_unscaled,
            PRICE_SCALE: self.price_scale,
            PRICE_UNSCALED: self.price_unscaled,
            TIME: self.time
        }

    def _to_decimal(self, integer, scale):
        return decimal.Decimal(integer) * decimal.Decimal(10) ** decimal.Decimal(scale)


