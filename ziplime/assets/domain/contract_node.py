from ziplime.assets.entities.exchange_asset import ExchangeAsset


class ContractNode:

    def __init__(self, contract: 'ExchangeAsset') -> None:
        self.contract = contract
        self.prev = None
        self.next = None

    def __rshift__(self, offset: int) -> "ContractNode | None":
        i = 0
        curr = self
        while i < offset and curr is not None:
            curr = curr.next
            i += 1
        return curr

    def __lshift__(self, offset: int) -> "ContractNode | None":
        i = 0
        curr = self
        while i < offset and curr is not None:
            curr = curr.prev
            i += 1
        return curr
