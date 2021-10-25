#include "json_es.h"
#include <cassert>
#include <iostream>
#include <sstream>
#include <cstring>
#include <stdexcept>
#include <cmath>
#define BACKSLASH 0x5c

using namespace std;

/*------------------- es_json Value ------------------*/
es_json::Value::Value(): _type(nullType), _data(""), _object(0), _array(0) {}

es_json::Value::Value(const Value& val): _type(val._type), _data(val._data){
    if(val._object){
        assert(val._type == ValueType::objectType);
        _object = new Object(*val._object);
    } else {
        _object = 0;
    }

    if(val._array){
        assert(val._type == ValueType::arrayType);
        _array = new Array(*val._array);
    } else {
        _array = 0;
    }

}

es_json::Value::~Value() {
    if(_object)
        delete _object;

    if(_array)
        delete _array;
}

/// Returns the data in es_json Format. Convert the values into string with escaped characters.
std::string es_json::Value::escapeJsonString(const std::string& input) {

    std::stringstream output;
    //std::cout << "-To Format: " << input << std::endl;

    for (auto iter = input.cbegin(); iter != input.cend(); iter++) {
        switch (*iter) {
            case '\\': output << "\\\\"; break;
            case '"': output << "\\\""; break;
            //case '/': output << "\\/"; break;
            case '\b': output << "\\b"; break;
            case '\f': output << "\\f"; break;
            case '\n': output << "\\n"; break;
            case '\r': output << "\\r"; break;
            case '\t': output << "\\t"; break;
            default: output << *iter; break;
        }
    }

    //std::cout << "+To Format: " << output.str() << std::endl;
    return output.str();
}

const char* es_json::Value::showType() const{

    switch (_type) {
        case es_json::Value::objectType :
            return "object";
        case es_json::Value::arrayType :
            return "array";
        case es_json::Value::stringType :
            return "string";
        case es_json::Value::booleanType :
            return "boolean";
        case es_json::Value::numberType :
            return "number";
        case es_json::Value::nullType :
            return "null";
        default:
            return "unknown";
    }
    return "--------------------";
}


const std::string& es_json::Value::getString() const {

    if(_type == stringType)
        return _data;

    throw std::logic_error("not a string");

    static const string error("error: is not of string type.");
    return error;
}

es_json::Value::operator const std::string&() const {
    return getString();
}


void es_json::Value::show() const {
    cout << *this;
}

bool es_json::Value::empty() const {

    switch(_type){
        case nullType:
            return true;

        case objectType:
            if(_object != 0)
                return _object->empty();
            break;

        case arrayType:
            if(_array != 0)
                return _array->empty();
            break;

        default:;
    }

    return false;
}

es_json::Value::operator bool() const {

    return getBoolean();
}

bool es_json::Value::getBoolean() const {
    switch(_type){
        case booleanType:
            assert(!_data.empty());
            return (_data[0] == 't');

        case numberType:
            return (getInt() != 0);

        case stringType:
            return (_data == "true");

        case nullType:
            return false;

        default:;
    }

    return false;
}

unsigned int es_json::Value::getUnsignedInt() const {
    if(_type == nullType)
        return 0;

    if(_type != numberType && _type != stringType)
        throw std::logic_error("not an unsigned int");

    unsigned int number = 0;
    istringstream ( _data ) >> number;
    return number;
}

int es_json::Value::getInt() const {

    if(_type == nullType)
        return 0.;

    if(_type != numberType && _type != stringType)
        throw std::logic_error("not an int");

    int number = 0;
    istringstream ( _data ) >> number;
    return number;
}

es_json::Value::operator int() const {
    return getInt();
}

long int es_json::Value::getLong() const {

    if(_type == nullType)
        return 0;

    if(_type != numberType && _type != stringType)
        throw std::logic_error("not a long int");

    long int number = 0;
    istringstream ( _data ) >> number;
    return number;
}

double es_json::Value::getDouble() const{

    if(_type == nullType)
        return 0.;

    if(_type != numberType && _type != stringType)
        throw std::logic_error("not a double");

    double number = 0;
    istringstream ( _data ) >> number;
    return number;
}

// Automatic cast in string.
es_json::Value::operator double() const{
    return getDouble();
}

float es_json::Value::getFloat() const{

    if(_type == nullType)
        return 0.;

    if(_type != numberType && _type != stringType)
        throw std::logic_error("not a float");

    float number = 0;
    istringstream ( _data ) >> number;
    return number;
}

// Automatic cast in string.
es_json::Value::operator float() const{
    return getFloat();
}

const es_json::Object& es_json::Value::getObject() const {
    if(_type != objectType){
        cout << "My Type: " << showType() << endl;
        cout << *this << endl;
        throw std::logic_error("not a es_json::Object");
    }

    if(_object == 0)
        throw std::runtime_error("no instance for es_json::Object");

    return *_object;
}

const es_json::Array& es_json::Value::getArray() const{
    if(_type != arrayType)
        throw std::logic_error("not a es_json::Array");

    if(_array == 0)
        throw std::runtime_error("no instance for es_json::Array");

    return *_array;
}

namespace es_json {
    std::ostream& operator<<(std::ostream& os, const Value& value){

        if(value._type == Value::objectType){
            assert(value._object != 0);
            os << *(value._object);
            return os;
        }

        if(value._type == Value::arrayType){
            assert(value._array != 0);
            os << *(value._array);
            return os;
        }

        if(value._type == Value::nullType){
            os << "null";
            return os;
        }

        if(value._type == Value::booleanType){
            if(value)
                os << "true";
            else
                os << "false";
            return os;
        }

        if(value._type == Value::stringType){
            os << "\"" << value._data << "\"";
            return os;
        }

        assert(value._type == Value::numberType);
        os << value._data;
        return os;
    }
}

// Set this value as a boolean.
void es_json::Value::setBoolean(bool b){
    _type = booleanType;
    if(b)
        _data = "true";
    else
        _data = "false";

    assert(_object == 0);
    assert(_array == 0);
}

// Set this value as a double.
void es_json::Value::setDouble(double v){

    _type = numberType;
    _data = std::to_string(v);

    assert(_object == 0);
    assert(_array == 0);
}

// Set this value as a int.
void es_json::Value::setInt(unsigned int u){
    _type = numberType;
    _data = std::to_string(u);

    assert(_object == 0);
    assert(_array == 0);
}

// Set this value as a int.
void es_json::Value::setInt(int i){
    _type = numberType;
    _data = std::to_string(i);

    assert(_object == 0);
    assert(_array == 0);
}

// Set this value as a long.
void es_json::Value::setLong(long l){
    _type = numberType;
    _data = std::to_string(l);

    assert(_object == 0);
    assert(_array == 0);
}

// Set this value as a string.
void es_json::Value::setString(const std::string& value){
    _type = stringType;
    _data = value;
    assert(_object == 0);
    assert(_array == 0);
}

/// Set this value as Object.
void es_json::Value::setObject(const es_json::Object& obj){
    assert(_type == nullType && _array == 0 && _object == 0);
    _type = objectType;
    _object = new Object(obj);
    _data = obj.str();
}

/// Set this value as Array.
void es_json::Value::setArray(const es_json::Array& array){
    assert(_type == nullType && _array == 0 && _object == 0);
    _type = arrayType;
    _array = new Array(array);
    _data = array.str();
}

const char* es_json::Value::read(const char* pCursor, const char* pEnd){

    // Call this function only once.
    assert(_data.empty());
    assert(_object == 0);
    assert(_array == 0);

    // Remove white spaces before the actual string.
    while(pCursor < pEnd && isspace(*pCursor)){
        ++pCursor;
    }
    const char* startPoint = pCursor;
    const char* endPoint = pCursor;

    // Interpret data.
    switch(*pCursor){

        case '}':
        case ']':
        case ',':
            _type = nullType;
            break;

        case 'n':
            _type = nullType;
            // Move until the end of the element, object or array.
            while(pCursor < pEnd && !isspace(*pCursor) && *pCursor != ',' && *pCursor != '}' && *pCursor != ']'){
                ++pCursor;
            }          
            endPoint = pCursor;
            break;

        case '-':
        case 'e':
        case '.':
        case 'E':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            _type = numberType;
            // Move until the end of the element, object or array.
            while(pCursor < pEnd && !isspace(*pCursor) && *pCursor != ',' && *pCursor != '}' && *pCursor != ']'){
                ++pCursor;
            }
            endPoint = pCursor;
            break;

        case 'f':
        case 't':
            _type = booleanType;
            // Move until the end of the element, object or array.
            while(pCursor < pEnd && !isspace(*pCursor) && *pCursor != ',' && *pCursor != '}' && *pCursor != ']'){
                ++pCursor;
            } 
            endPoint = pCursor;
            break;

        case '"':
            // Define type
            _type = stringType;

            // Don't store the quote
            ++startPoint;

            // Move to next character
            ++pCursor;

            // Move until the end of the string.
            while(pCursor < pEnd){
                if(*pCursor == '"' && *(pCursor - 1) != BACKSLASH)
                    break;
                else
                    ++pCursor;
            }
            endPoint = pCursor;
            ++pCursor;

            #ifndef NDEBUG
            if(!(isspace(*pCursor) || *pCursor == ',' || *pCursor == '}' || *pCursor == ']'))
                printf("Debug Read 2: %s\n", pCursor);
            #endif

            assert( isspace(*pCursor) || *pCursor == ',' || *pCursor == '}' || *pCursor == ']');
            break;

        case '{':
            // Only one type is allowed.
            assert(_array == 0 && _object == 0);
            _type = objectType;
            _object = new Object;
            pCursor = _object->addMember(pCursor, pEnd);
            endPoint = pCursor;
            break;

        case '[':
            // Only one type is allowed.
            assert(_array == 0 && _object == 0);
            _type = arrayType;
            _array = new Array;
            pCursor = _array->addElement(pCursor, pEnd);
            endPoint = pCursor;
            break;

        default:
            throw std::logic_error("illformed JSON.");
    }

    assert(pCursor <= pEnd);
    assert(pCursor >= startPoint);
    assert(pCursor >= endPoint);

    if(endPoint == startPoint)
        _data = "";

    if(endPoint > startPoint)
        _data = std::string(startPoint, endPoint - startPoint);

    return pCursor;
}

bool es_json::Value::operator==(const es_json::Value& v) const {

    if( _type != v._type )
        return false;

    // Both null types.
    if( _type == nullType)
        return true;

    if( _object && !v._object )
        return false;

    if( !_object && v._object )
        return false;

    if( _array && !v._array )
        return false;

    if( !_array && v._array )
        return false;

    if( _object && v._object )
        return ( *_object == *v._object );

    if( _array && v._array )
        return ( *_array == *v._array );

    if( _type == booleanType)
        return ( _data[0] == v._data[0] );

    // Check only as double.
    if( _type == numberType){
        double number1 = 0.0;
        double number2 = 0.0;
        istringstream (   _data ) >> number1;
        istringstream ( v._data ) >> number2;
        return ( fabs(number1 - number2) < 1e-9 );
    }

    if( _data != v._data )
        return false;

    return true;
}

// Weak equality that can compare value of different types.
bool es_json::Value::weakEquality(const es_json::Value& a, const es_json::Value& b){

    // Compare Null and Numbers
    if(a._type == nullType && b._type == numberType)
        return (fabs(b.getDouble()) < 1e-9);

    if(b._type == nullType && a._type == numberType)
        return (fabs(a.getDouble()) < 1e-9);

    // Compare Null and Boolean
    if(a._type == nullType && b._type == booleanType)
        return !(b.getBoolean());

    if(b._type == nullType && a._type == booleanType)
        return !(a.getBoolean());

    // Compare Null and Object
    if(a._type == nullType && b._type == objectType){
        if(b._object == 0 || b._object->empty())
            return true;
        else
            return false;
    }

    if(b._type == nullType && a._type == objectType){
        if(a._object == 0 || a._object->empty())
            return true;
        else
            return false;
    }

    // Compare Null and Array
    if(a._type == nullType && b._type == arrayType){
        if(b._array == 0 || b._array->empty())
            return true;
        else
            return false;
    }

    if(b._type == nullType && a._type == arrayType){
        if(a._array == 0 || a._array->empty())
            return true;
        else
            return false;
    }

    // Compare Null and String
    if(a._type == nullType && b._type == stringType){
        if(b._data.empty() || b._data == "" || b._data == "null")
            return true;
        else
            return false;
    }

    if(b._type == nullType && a._type == stringType){
        if(a._data.empty() || a._data == "" || a._data == "null")
            return true;
        else
            return false;
    }

    return (a == b);
}

/*------------------- es_json Object ------------------*/
es_json::Object::Object(){

}

es_json::Object::Object(const Object& obj): _memberMap(obj._memberMap){

}

/// Loops over the string and splits into members.
const char* es_json::Object::addMember(const char* startStr, const char* endStr){

    // Means it starts with {
    if(startStr[0] != '{')
        throw std::logic_error("Object illformed, does not start with {");

    ++startStr;

    // Loop over members.
    while(startStr <= endStr) {

        // Key start and end point.
        const char* pKeyStart = 0;
        const char* pKeyEnd = 0;

        // Remove white spaces until we find the key.
        while(startStr <= endStr && isspace(*startStr)){
            ++startStr;
        }
        // The object is empty.
        if(*startStr == '}')
            break;

        pKeyStart = ++startStr;

        // Loop until the end of the key.
        while(startStr <= endStr){

            if(*startStr == '"'  && *(startStr - 1) != BACKSLASH){
                pKeyEnd = startStr - 1;
                ++startStr;
                break;
            }

            ++startStr;
        }

        // Loop to move to the beginning of the value.
        while(startStr <= endStr){

            if(*startStr == ':'){
                ++startStr;
                break;
            }

            ++startStr;
        }

        // We must never reach the end of the string.
        if(*startStr == '\0')
            throw std::logic_error("Object illformed, end of the string reached.");

        // Get the end of the value.
        startStr = _memberMap[ Key(pKeyStart, pKeyEnd - pKeyStart + 1) ].read(startStr, endStr);

        // Remove white spaces at the end of the value if any.
        while(isspace(*startStr)){
            ++startStr;
        }

        // We reached the end of the child member.
        if(*startStr == '}')
          break;

        if(*startStr != ',')
            throw std::logic_error("Object illformed, missing coma object separator.");

        // Move to next character.
        ++startStr;
    }

    if(*startStr != '}')
        throw std::logic_error("Object illformed, does not end with }");

    ++startStr;

    assert(startStr <= endStr);

    // Returns the consummed size
    return startStr;
}

// Add a string member
void es_json::Object::addMemberByKey(const string& key, const string& str){
    // Add a value as string
    _memberMap[key].setString(es_json::Value::escapeJsonString(str));
}

// Add member by key value.
void es_json::Object::addMemberByKey(const std::string& key, const es_json::Array& array){
    // Add a value as array
    _memberMap[key].setArray(array);
}

// Add member by key value.
void es_json::Object::addMemberByKey(const std::string& key, const es_json::Value& value){
    _memberMap[key] = value;
}

// Add member by key value.
void es_json::Object::addMemberByKey(const std::string& key, const es_json::Object& obj){
    // Add a value as object
    _memberMap[key].setObject(obj);
}

// Add member by key value.
void es_json::Object::addMemberByKey(const std::string& key, double v){
    // Add a value as object
    _memberMap[key].setDouble(v);
}

/// Add member by key value.
void es_json::Object::addMemberByKey(const std::string& key, bool v){
    // Add a value as boolean
    _memberMap[key].setBoolean(v);
}

/// Add member by key value.
void es_json::Object::addMemberByKey(const std::string& key, const char* s){
    // Add a value as string
    _memberMap[key].setString(es_json::Value::escapeJsonString(s));
}

/// Add member by key value.
void es_json::Object::addMemberByKey(const std::string& key, unsigned int u){
    // Add a value as int
    _memberMap[key].setInt(u);
}

/// Add member by key value.
void es_json::Object::addMemberByKey(const std::string& key, int i){
    // Add a value as int
    _memberMap[key].setInt(i);
}

/// Add member by key value.
void es_json::Object::addMemberByKey(const std::string& key, long i){
    // Add a value as int
    _memberMap[key].setLong(i);
}

/// Add member by key value.
void es_json::Object::addMemberByKey(const std::string& key, unsigned long i){
    // Add a value as int
    _memberMap[key].setLong(i);
}


// Tells if member exists.
bool es_json::Object::member(const string& key) const {
    return (_memberMap.find(key) != _memberMap.end());
}

// Append another object to this one.
void es_json::Object::append(const es_json::Object& obj){

    for(std::map< Key, Value >::const_iterator cit = obj._memberMap.cbegin(); cit != obj._memberMap.cend(); ++cit){

        if(_memberMap.find(cit->first) != _memberMap.end())
            throw std::logic_error("Cannot merge objects: one key appears in both.");

        _memberMap.insert(std::make_pair(cit->first, cit->second));
    }
}

// Return the value of the member[key], key must exist in the map.
const es_json::Value& es_json::Object::getValue(const std::string& key) const {

    map< Key, Value >::const_iterator it = _memberMap.find(key);

    if(it == _memberMap.end()){
        throw std::logic_error("failed finding key.");
    }

    return it->second;
}

// Return the value of the member[key], does not test if exists.
const es_json::Value& es_json::Object::operator[](const std::string& key) const noexcept{
    return _memberMap.find(key)->second;
}


/// Returns the data in es_json Format.
std::string es_json::Object::str() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

bool es_json::Object::contain(const Object& o) const {

    for(const std::pair< Key, Value >& p : o._memberMap){
        std::map< Key, Value >::const_iterator cit = _memberMap.find(p.first);
        if( cit == _memberMap.end() ){
            return false;
        }
        if( cit->second != p.second){
            return false;
        }
    }

    return true;
}

bool es_json::Object::operator==(const Object& o) const {

    if(_memberMap.size() != o._memberMap.size())
        return false;

    return contain(o);
}

// Output in es_json format
namespace es_json {
    std::ostream& operator<<(std::ostream& os, const Object& obj){
        os << "{";
        for(std::map< Key, Value >::const_iterator it = obj._memberMap.begin(); it != obj._memberMap.end(); ){
            os << "\"" << it->first << "\":" << it->second;
            ++it;
            if(it != obj._memberMap.end())
                os << ",";
        }
        os << "}";

        return os;
    }
}

/*------------------- es_json Array ------------------*/
// Constructor.
es_json::Array::Array(){

}

// Loops over the string, splits into elements and returns the consummed size
const char* es_json::Array::addElement(const char* pStart, const char* pEnd){

    // Means it starts with [
    assert(pStart[0] == '[');
    ++pStart;

    // Remove white spaces until we find the first value.
    while(pStart <= pEnd && isspace(*pStart)){
        ++pStart;
    }
    
    // The array is empty.
    if(*pStart == ']')
        return ++pStart;

    while(pStart <= pEnd){
        _elementList.push_back(Value());
        pStart = _elementList.back().read(pStart, pEnd);

        // Remove white spaces until we find the end of the array or the next value.
        while(pStart <= pEnd && isspace(*pStart)){
            ++pStart;
        }
            
        if(*pStart == ']')
            break;

        assert(*pStart == ',' || pStart > pEnd);
        ++pStart;
    }

    ++pStart;

   assert(pStart <= pEnd);

    // Returns the consummed size
    return pStart;
}

// Copy and add this value to the list.
void es_json::Array::addElement(const es_json::Value& val){
    _elementList.push_back(val);
}

/// Copy the object to a value and add this value to the list.
void es_json::Array::addElement(const es_json::Object& obj){
    _elementList.push_back(Value());
    _elementList.back().setObject(obj);
}

bool es_json::Array::operator==(const Array& a) const {

    for(const Value& v0 : a._elementList){
        bool found = false;

        for(const Value& v1 : _elementList){

            if(v0 == v1){
                found = true;
                break;
            }
        }

        if(!found){
            return false;
        }
    }
    return true;
}

// Output in es_json pretty format
namespace es_json {
    std::ostream& operator<<(std::ostream& os, const Array& array){

        os << "[";

        list<Value>::const_iterator it = array._elementList.begin();
        if(it != array._elementList.end())
            os << *it;

        for(++it; it != array._elementList.end(); ++it){
            os << "," << *it;
        }
            
        os << ']';

        return os;
    }
}

// Returns the data in es_json Format.
std::string es_json::Array::str() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

/// Methods for pretty formatted output.

#define NORMAL (char)27 << "[0;24m"
#define BOLD (char)27 << "[1m"
#define GREEN (char)27 << "[32m"
#define YELLOW (char)27 << "[33m"

// Output es_json in a pretty format.
std::string es_json::Value::pretty(int tab) const {

    std::ostringstream oss;

    if(isObject() && _object != 0)
        return _object->pretty(tab);

    if(isArray() && _array != 0)
        return _array->pretty(tab);

    oss << YELLOW << " " << *this << NORMAL;
    return oss.str();
}

// Output es_json in a pretty format.
std::string es_json::Array::pretty(int tab) const {

    std::ostringstream tabStream;
    for(int i = 0; i < tab; ++i){
        tabStream << "\t";
    }
    std::ostringstream oss;
    oss << " [\n";
    for(std::list< Value >::const_iterator it = _elementList.begin(); it != _elementList.end(); ){
        oss << tabStream.str() << it->pretty(tab);
        ++it;
        if(it != _elementList.end())
            oss << ",\n";
    }
    oss << "\n" << tabStream.str() << "]";

    return oss.str();
}

// Output es_json in a pretty format with same colors as Marvel/Sense.
std::string es_json::Object::pretty(int tab) const{
    std::ostringstream tabStream;
    for(int i = 0; i < tab; ++i){
        tabStream << "\t";
    }
    std::ostringstream oss;
    oss << " {\n";
    for(std::map< Key, Value >::const_iterator it = _memberMap.begin(); it != _memberMap.end(); ){

        oss << GREEN << BOLD << tabStream.str() << "\"" << it->first << "\"" << NORMAL << ":";
        oss << it->second.pretty(tab+1);
        ++it;
        if(it != _memberMap.end())
            oss << ",\n";
    }
    oss << "\n" << tabStream.str() << "}";

    return oss.str();
}
