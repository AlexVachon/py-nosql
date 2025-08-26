class DocumentWrapper:
    def __init__(self, doc_id, doc):
        self._id = doc_id
        self._doc = doc

    def __getattr__(self, item):
        return getattr(self._doc, item)

    def __repr__(self):
        return f"<Document _id={self._id}, {self._doc}>"