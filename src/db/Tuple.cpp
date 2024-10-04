#include <cstring>
#include <db/Tuple.hpp>
#include <stdexcept>
#include <set>

using namespace db;

Tuple::Tuple(const std::vector<field_t> &fields) : fields(fields) {}

type_t Tuple::field_type(size_t i) const {
  const field_t &field = fields.at(i);
  if (std::holds_alternative<int>(field))
    return type_t::INT;
  if (std::holds_alternative<double>(field))
    return type_t::DOUBLE;
  if (std::holds_alternative<std::string>(field))
    return type_t::CHAR;
  
  throw std::logic_error("Unknown field type");
}

size_t Tuple::size() const { return fields.size(); }

const field_t &Tuple::get_field(size_t i) const { return fields.at(i); }

TupleDesc::TupleDesc(const std::vector<type_t> &types,
		     const std::vector<std::string> &names) {
  if (types.size() != names.size())
    throw std::invalid_argument("Types and types must have the same length.");

  std::set<std::string> unique_names(names.begin(), names.end());

  if (unique_names.size() != names.size())
    throw std::invalid_argument("Names must be unique.");    

  this->types = types;
  this->names = names;
}

bool TupleDesc::compatible(const Tuple &tuple) const {
  size_t i;
  
  if (types.size() != tuple.size())
    return false;
  
  for (i=0; i<types.size(); ++i) {
    if (types[i] != tuple.field_type(i))
      return false;
  }

  return true;
}

size_t TupleDesc::index_of(const std::string &name) const {
  size_t i;

  for (i=0; i<names.size(); ++i) {
    if (names[i] == name)
      return i;
  }
  throw std::logic_error("Not found");  
}

size_t type_size(const type_t &t1) {
  if (t1 == type_t::INT)
    return db::INT_SIZE;
  if (t1 == type_t::DOUBLE)
    return db::DOUBLE_SIZE;
  if (t1 == type_t::CHAR)
    return db::CHAR_SIZE;
  
  throw std::logic_error("Unkown type in TupleDesc.");
}

size_t TupleDesc::offset_of(const size_t &index) const {
  size_t i, offset=0;

  if (index >= types.size())
    throw std::invalid_argument("Index out of bounds.");

  for (i=0; i<index; ++i)
    offset += type_size(types[i]);

  return offset;
}

size_t TupleDesc::length() const { // length of tuple in mem=names+els
  size_t l=0;
  
  for (const auto &t1:types)
    l += type_size(t1);

  return l;
}

size_t TupleDesc::size() const {
  return types.size();		// number of elements in tuple
}

Tuple TupleDesc::deserialize(const uint8_t *data) const {
	size_t i,
		d_index=0, d_size;
	std::vector<field_t> fields;

	for (i=0; i < types.size(); ++i) { // first byte is num_types
		switch (types[i]) {
		case type_t::INT: {
			d_size = INT_SIZE;
			int field;
			std::memcpy(&field, &data[d_index], d_size);
			fields.push_back(field);
			break;
		}
		case type_t::DOUBLE: {
			d_size = DOUBLE_SIZE;
			double field;
			std::memcpy(&field, &data[d_index], d_size);
			fields.push_back(field);
			break;
		}
		case type_t::CHAR: {
			d_size = CHAR_SIZE;
			fields.push_back(std::string((char*)&data[d_index]));
			break;
		}
		default:
			throw std::logic_error("Unknown field type! " + std::to_string(data[i]));
		}

		d_index += d_size;  // Move to the next field in data
	}

	return Tuple(fields);  // Return a tuple with deserialized fields
}

// just serialize elements themselves since TupleDesc contains rest of info
void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
  size_t i,
		d_index=0, d_size;

  for (i=0; i<types.size(); ++i) { // data
    const field_t &field = t.get_field(i);
    
    switch(types[i]) {
    case type_t::INT: {
      int d = std::get<int>(field);
      d_size = INT_SIZE;
      std::memcpy(&data[d_index], &d, d_size);
			break;
    }
    case type_t::DOUBLE: {
      double d = std::get<double>(field);
      d_size = DOUBLE_SIZE;
      std::memcpy(&data[d_index], &d, d_size);
			break;
    }
    case type_t::CHAR: {
      std::string d = std::get<std::string>(field);
      d_size = CHAR_SIZE;
      std::memcpy(&data[d_index], &d, d.size());
			break;
    }
    default:
      throw std::logic_error("Unknown field type");      
    }

    d_index += d_size;
  }
}

db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
  std::vector<type_t> merged_types = td1.types;
  std::vector<std::string> merged_names = td1.names;

  merged_types.insert(merged_types.end(), td2.types.begin(), td2.types.end());
  merged_names.insert(merged_names.end(), td2.names.begin(), td2.names.end());

  return TupleDesc(merged_types, merged_names);
}
