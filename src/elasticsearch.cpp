#include <iostream>
#include <sstream>
#include <cstring>
#include <cassert>
#include <locale>
#include <vector>
#include "elasticsearch.h"

ElasticSearch::ElasticSearch(const std::string& node, bool readOnly): _http(node, false), _readOnly(readOnly) {
    // std::cout<<"调用Elasticsearch 构造函数"<<std::endl;
    // Test if instance is active.
    if(!isActive())
        EXCEPTION("Cannot create engine, database is not active.");
    
}

ElasticSearch::~ElasticSearch() {
}

// Test connection with node.
bool ElasticSearch::isActive() {

    es_json::Object root;

    // try {
        _http.get(0, 0, &root);
    // }
    // catch(Exception& e){
    //     printf("get(0) failed in ElasticSearch::isActive(). Exception caught: %s\n", e.what());
    //     return false;
    // }
    // catch(std::exception& e){
    //     printf("get(0) failed in ElasticSearch::isActive(). std::exception caught: %s\n", e.what());
    //     return false;
    // }
    // catch(...){
    //     printf("get(0) failed in ElasticSearch::isActive().\n");
    //     return false;
    // }

    if(root.empty())
        return false;

    if(!root.member("status") || root["status"].getInt() != 200){
        printf("Status is not 200. Cannot find Elasticsearch Node.\n");
        return false;
    }

    return true;
}

// Request the document by index/type/id.
bool ElasticSearch::getDocument(const char* index, const char* type, const char* id, es_json::Object& msg){
    std::ostringstream oss;
    oss << index << "/" << type << "/" << id;
    _http.get(oss.str().c_str(), 0, &msg);
    return msg["found"];
}

// Request the document by index/type/ query key:value.
void ElasticSearch::getDocument(const std::string& index, const std::string& type, const std::string& key, const std::string& value, es_json::Object& msg){
    std::ostringstream oss;
    oss << index << "/" << type << "/_search";
    std::stringstream query;
    query << "{\"query\":{\"match\":{\""<< key << "\":\"" << value << "\"}}}";
    _http.post(oss.str().c_str(), query.str().c_str(), &msg);
}

/// Delete the document by index/type/id.
bool ElasticSearch::deleteDocument(const char* index, const char* type, const char* id){
    if(_readOnly)
        return false;

    std::ostringstream oss;
    oss << index << "/" << type << "/" << id;
    es_json::Object msg;
    _http.remove(oss.str().c_str(), 0, &msg); 
    return std::string(msg.getValue("result"))=="deleted";
}

/// Delete the document by index/type.
bool ElasticSearch::deleteAll(const char* index, const char* type){
    if(_readOnly)
        return false;

    std::ostringstream uri, data;
    uri << index << "/" << type << "/_query";
    data << "{\"query\":{\"match_all\": {}}}";
    es_json::Object msg;
    _http.remove(uri.str().c_str(), data.str().c_str(), &msg);

    if(!msg.member("_indices") || !msg["_indices"].getObject().member(index) || !msg["_indices"].getObject()[index].getObject().member("_shards"))
        return false;

    if(!msg["_indices"].getObject()[index].getObject()["_shards"].getObject().member("failed"))
        return false;

    return (msg["_indices"].getObject()[index].getObject()["_shards"].getObject()["failed"].getInt() == 0);
}

// Request the document number of type T in index I.
long unsigned int ElasticSearch::getDocumentCount(const char* index, const char* type){
    std::ostringstream oss;
    oss << index << "/" << type << "/_count";
    es_json::Object msg;
    _http.get(oss.str().c_str(),0,&msg);

    size_t pos = 0;
    if(msg.member("count"))
        pos = msg.getValue("count").getUnsignedInt();
    else
        printf("We did not find \"count\" member.\n");

    return pos;
}

// Test if document exists
bool ElasticSearch::exist(const std::string& index, const std::string& type, const std::string& id){
    std::stringstream url;
    url << index << "/" << type << "/" << id;

    es_json::Object result;
    _http.get(url.str().c_str(), 0, &result);

    if(!result.member("found")){
        // std::cout << result << std::endl;
        EXCEPTION("Database exception, field \"found\" must exist.");
    }

    return result.getValue("found");
}

// 手动指定id，插入一个 document
/// Index a document.
bool ElasticSearch::index(const std::string& index, const std::string& type, const std::string& id, const es_json::Object& jData){

    if(_readOnly)
        return false;

    std::stringstream url; 
    url << index << "/" << type << "/" << id;

    std::stringstream data;
    data << jData;

    es_json::Object result;
    // put()方法在http.h中定义
    _http.put(url.str().c_str(), data.str().c_str(), &result);
    // std::cout<<"149:"<<result<<std::endl;
    if(!result.member("result"))
        EXCEPTION("The index induces error.");

    if(int(result.getValue("status"))==200 || int(result.getValue("status"))==201){
        return true;
    }
        
    // std::cout << "endPoint: " << index << "/" << type << "/" << id << std::endl;
    // std::cout << "jData" << jData.pretty() << std::endl;
    // std::cout << "result" << result.pretty() << std::endl;

    EXCEPTION("The index returns ok: false.");
    return false;
}

// 自动创建id，插入一个 document
/// Index a document with automatic id creation
std::string ElasticSearch::index(const std::string& index, const std::string& type, const es_json::Object& jData){

    if(_readOnly)
        return "";

    std::stringstream url;
    url << index << "/" << type << "/";

    std::stringstream data;
    data << jData;

    es_json::Object result;
    _http.post(url.str().c_str(), data.str().c_str(), &result);

    if(!result.member("result")){
        if(result["result"].getString() != "created"){
            // std::cout << "url: " << url.str() << std::endl;
            // std::cout << "data: " << data.str() << std::endl;
            // std::cout << "result: " << result.str() << std::endl;
            EXCEPTION("The index induces error.");
        }
    }

    return result.getValue("_id").getString();
}

// Update a document field.
bool ElasticSearch::update(const std::string& index, const std::string& type, const std::string& id, const std::string& key, const std::string& value){
    if(_readOnly)
        return false;

    std::stringstream url;
    url << index << "/" << type << "/" << id << "/_update";

    std::stringstream data;
    data << "{\"doc\":{\"" << key << "\":\""<< value << "\"}}";

    es_json::Object result;
    _http.post(url.str().c_str(), data.str().c_str(), &result);

    if(!result.member("_version"))
        EXCEPTION("The update failed.");

    return true;
}

// Update doccument fields.
bool ElasticSearch::update(const std::string& index, const std::string& type, const std::string& id, const es_json::Object& jData){
    if(_readOnly)
        return false;

    std::stringstream url;
    url << index << "/" << type << "/" << id << "/_update";

    std::stringstream data;
    data << "{\"doc\":" << jData;
    data << "}";

    es_json::Object result;
    _http.post(url.str().c_str(), data.str().c_str(), &result);

    if(result.member("error"))
        EXCEPTION("The update doccument fields failed.");

    return true;
}

// Update or insert if the document does not already exists.
bool ElasticSearch::upsert(const std::string& index, const std::string& type, const std::string& id, const es_json::Object& jData){

    if(_readOnly)
        return false;

    std::stringstream url;
    url << index << "/" << type << "/" << id << "/_update";

    std::stringstream data;
    data << "{\"doc\":" << jData;
    data << ", \"doc_as_upsert\" : true}";

    es_json::Object result;
    _http.post(url.str().c_str(), data.str().c_str(), &result);

    if(result.member("error"))
        EXCEPTION("The update doccument fields failed.");

    return true;
}

/// Search API of ES.
long ElasticSearch::search(const std::string& index, const std::string& type, const std::string& query, es_json::Object& result){

    std::stringstream url;
    if (type.empty()){
        url << index << "/" << "/_search";
    }else{
        url << index << "/" << type << "/_search";
    }
    // std::cout<<"url:"<<url.str().c_str()<<std::endl;
    _http.post(url.str().c_str(), query.c_str(), &result);

    if(!result.member("timed_out")){
        std::cout << url.str() << " -d " << query << std::endl;
        std::cout << "result: " << result << std::endl;
        EXCEPTION("Search failed.");
    }

    if(result.getValue("timed_out")){
        // std::cout << "result: " << result << std::endl;
        EXCEPTION("Search timed out.");
    }
    // std::cout<<"273 result:"<<result.pretty()<<std::endl;
    return result.getValue("hits").getObject().getValue("total").getObject().getValue("value").getLong();
}


bool ElasticSearch::remove(const std::string& index, const std::string& type, const std::string& query, es_json::Object& result){

    std::stringstream url;
    if (type.empty()){
        url << index << "/" << "/_delete_by_query";
    }else{
        url << index << "/" << type << "/_delete_by_query";
    }
    // std::cout<<"url:"<<url.str().c_str()<<std::endl;
    _http.post(url.str().c_str(), query.c_str(), &result);

    if(!result.member("timed_out")){
        // std::cout << url.str() << " -d " << query << std::endl;
        // std::cout << "result: " << result << std::endl;
        EXCEPTION("Delete failed.");
    }

    if(result.getValue("timed_out")){
        // std::cout << "result: " << result << std::endl;
        EXCEPTION("Delete timed out.");
    }
    // std::cout<<"273 result:"<<result<<std::endl;
    return result.getValue("deleted").getInt()==1;
}

/// Delete given type (and all documents, mappings)
bool ElasticSearch::deleteType(const std::string& index, const std::string& type){
    std::ostringstream uri;
    uri << index << "/" << type;
    return (200 == _http.remove(uri.str().c_str(), 0, 0));
}

// Test if index exists
bool ElasticSearch::exist(const std::string& index){
    return (200 == _http.head(index.c_str(), 0, 0));
}

// Create index, optionally with data (settings, mappings etc)
bool ElasticSearch::createIndex(const std::string& index, const char* data){
    return (200 == _http.put(index.c_str(), data, 0));
}

// Delete given index (and all types, documents, mappings)
bool ElasticSearch::deleteIndex(const std::string& index){
    return (200 == _http.remove(index.c_str(), 0, 0));
}

// Refresh the index.
void ElasticSearch::refresh(const std::string& index){
    std::ostringstream oss;
    oss << index << "/_refresh";

    es_json::Object msg;
    _http.get(oss.str().c_str(), 0, &msg);
}

bool ElasticSearch::initScroll(std::string& scrollId, const std::string& index, const std::string& type, const std::string& query, int scrollSize) {
    std::ostringstream oss;
    oss << index << "/" << type << "/_search?scroll=1m&search_type=scan&size=" << scrollSize;

    es_json::Object msg;
    if (200 != _http.post(oss.str().c_str(), query.c_str(), &msg))
        return false;
    
    scrollId = msg["_scroll_id"].getString();
    return true;
}

bool ElasticSearch::scrollNext(std::string& scrollId, es_json::Array& resultArray) {
    es_json::Object msg;
    if (200 != _http.post("/_search/scroll?scroll=1m", scrollId.c_str(), &msg))
        return false;
    
    scrollId = msg["_scroll_id"].getString();
    
    appendHitsToArray(msg, resultArray);
    return true;
}

void ElasticSearch::clearScroll(const std::string& scrollId) {
    _http.remove("/_search/scroll", scrollId.c_str(), 0);
}

int ElasticSearch::fullScan(const std::string& index, const std::string& type, const std::string& query, es_json::Array& resultArray, int scrollSize) {
    resultArray.clear();
    
    std::string scrollId;
    if (!initScroll(scrollId, index, type, query, scrollSize))
        return 0;

    size_t currentSize=0, newSize=0;
    while (scrollNext(scrollId, resultArray))
    {
        newSize = resultArray.size();
        if (currentSize == newSize)
            break;
        
        currentSize = newSize;
    }
    return currentSize;
}

void ElasticSearch::appendHitsToArray(const es_json::Object& msg, es_json::Array& resultArray) {

    if(!msg.member("hits"))
        EXCEPTION("Result corrupted, no member \"hits\".");

    if(!msg.getValue("hits").getObject().member("hits"))
        EXCEPTION("Result corrupted, no member \"hits\" nested in \"hits\".");

    for(const es_json::Value& value : msg["hits"].getObject()["hits"].getArray()) {
        resultArray.addElement(value);
    }
}

// Bulk API of ES.
bool ElasticSearch::bulk(const char* data, es_json::Object& jResult) {
	 if(_readOnly)
		return false;

	return (200 == _http.post("/_bulk", data, &jResult));
}

BulkBuilder::BulkBuilder() {}

void BulkBuilder::createCommand(const std::string &op, const std::string &index, const std::string &type, const std::string &id = "") {
	es_json::Object command;
	es_json::Object commandParams;

	if (id != "") {
		commandParams.addMemberByKey("_id", id);
	}

	commandParams.addMemberByKey("_index", index);
	commandParams.addMemberByKey("_type", type);

	command.addMemberByKey(op, commandParams);
	operations.push_back(command);
}

void BulkBuilder::index(const std::string &index, const std::string &type, const std::string &id, const es_json::Object &fields) {
	createCommand("index", index, type, id);
	operations.push_back(fields);
}

void BulkBuilder::create(const std::string &index, const std::string &type, const std::string &id, const es_json::Object &fields) {
	createCommand("create", index, type, id);
	operations.push_back(fields);
}

void BulkBuilder::index(const std::string &index, const std::string &type, const es_json::Object &fields) {
	createCommand("index", index, type);
	operations.push_back(fields);
}

void BulkBuilder::create(const std::string &index, const std::string &type, const es_json::Object &fields) {
	createCommand("create", index, type);
	operations.push_back(fields);
}

void BulkBuilder::update(const std::string &index, const std::string &type, const std::string &id, const es_json::Object &body) {
    createCommand("update", index, type, id);
    operations.push_back(body);
}

void BulkBuilder::update_doc(const std::string &index, const std::string &type, const std::string &id, const es_json::Object &fields, bool upsert) {
	createCommand("update", index, type, id);

	es_json::Object updateFields;
	updateFields.addMemberByKey("doc", fields);
    updateFields.addMemberByKey("doc_as_upsert", upsert);

	operations.push_back(updateFields);
}

void BulkBuilder::del(const std::string &index, const std::string &type, const std::string &id) {
	createCommand("delete", index, type, id);
}

std::string BulkBuilder::str() {
	std::stringstream json;

	for(auto &operation : operations) {
		json << operation.str() << std::endl;
	}

	return json.str();
}

void BulkBuilder::clear() {
	operations.clear();
}

bool BulkBuilder::isEmpty() {
	return operations.empty();
}
