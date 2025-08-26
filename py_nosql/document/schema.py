from typing import Dict

class Schema:
    def __init__(self, fields: dict):
        """
        fields example:
        {
            "name": {"type": str, "unique": True, "length": {"gt": 3}},
            "age": {"type": int, "$gte": 0, "$lte": 100},
            "role": {"type": str, "enum": ["admin", "member", "guest"]},
            "user_id": {"type": str, "ref": "users"}  # references another collection
        }
        """
        self.fields = fields
        # track unique values in memory
        self._unique_values = {f: set() for f, rules in fields.items() if rules.get("unique")}

    def validate(self, doc: dict, existing_docs: list = None, collections: dict = None):
        """
        Validate a document against the schema.
        :param doc: the document to validate
        :param existing_docs: list of existing documents for unique constraint
        :param collections: dict of collection_name -> Collection, used for 'ref' checks
        """
        existing_docs = existing_docs or []
        collections = collections or {}

        for field, rules in self.fields.items():
            val = doc.get(field)

            # skip missing fields (optional)
            if val is None:
                continue

            # Type check
            if "type" in rules and not isinstance(val, rules["type"]):
                raise TypeError(f"Field '{field}' must be of type {rules['type'].__name__}, got {type(val).__name__}")

            # Unique constraint
            if rules.get("unique"):
                for d in existing_docs:
                    if d.get(field) == val:
                        raise ValueError(f"Duplicate value for unique field '{field}': {val}")
                if val in self._unique_values[field]:
                    raise ValueError(f"Duplicate value for unique field '{field}': {val}")

            # Length constraint for strings
            if isinstance(val, str) and "length" in rules:
                length_rules = rules["length"]
                if "gt" in length_rules and not len(val) > length_rules["gt"]:
                    raise ValueError(f"Field '{field}' length must be > {length_rules['gt']}")
                if "gte" in length_rules and not len(val) >= length_rules["gte"]:
                    raise ValueError(f"Field '{field}' length must be >= {length_rules['gte']}")
                if "lt" in length_rules and not len(val) < length_rules["lt"]:
                    raise ValueError(f"Field '{field}' length must be < {length_rules['lt']}")
                if "lte" in length_rules and not len(val) <= length_rules["lte"]:
                    raise ValueError(f"Field '{field}' length must be <= {length_rules['lte']}")

            # Numeric constraints
            if isinstance(val, (int, float)):
                for op in ["$gt", "$gte", "$lt", "$lte"]:
                    if op in rules:
                        if op == "$gt" and not val > rules[op]:
                            raise ValueError(f"Field '{field}' must be > {rules[op]}")
                        if op == "$gte" and not val >= rules[op]:
                            raise ValueError(f"Field '{field}' must be >= {rules[op]}")
                        if op == "$lt" and not val < rules[op]:
                            raise ValueError(f"Field '{field}' must be < {rules[op]}")
                        if op == "$lte" and not val <= rules[op]:
                            raise ValueError(f"Field '{field}' must be <= {rules[op]}")

            # Enum / allowed values
            if "enum" in rules:
                if val not in rules["enum"]:
                    raise ValueError(f"Field '{field}' must be one of {rules['enum']}, got '{val}'")

            # Reference check
            if "ref" in rules:
                ref_collection_name = rules["ref"]
                ref_collection = collections.get(ref_collection_name)
                if ref_collection and not ref_collection.get(val):
                    raise ValueError(f"Field '{field}' references a non-existent document ID '{val}' in '{ref_collection_name}'")

        return True

    def register(self, doc: dict):
        """Mark unique fields as used."""
        for f in self._unique_values:
            val = doc.get(f)
            if val is not None:
                self._unique_values[f].add(val)
