#include <iostream>
#include "elasticsearch.h"
#include <map>
#include <list>
int main(int, char**) {
    // 初始化一个es库客户端
    // Instanciate elasticsearch client. 
    const std::string ip = "*****:9200";
    // ElasticSearch es(ip);
    ElasticSearch* es;
    es = new ElasticSearch(ip);
    std::cout<<"初始化完成！"<<std::endl;
    // 创建一个文档
    // Index one document.
    // Json::Object jData;
    // jData.addMemberByKey("doc_name", "海淀公园.docx");
    // jData.addMemberByKey("sentence_id", "1");
    // jData.addMemberByKey("sentence", "海淀公园修建于清朝年间");

     
    // 插入文档
    // if(!es->index("twitter", "tweet", "1", jData))
    //     std::cerr << "Index failed." << std::endl;
    // std::cerr << "\n插入文档成功." << std::endl; 

    // Get document.
    // Json::Object jResult;
    // if(!es->getDocument("twitter", "tweet", "1", jResult))
    //     std::cerr << "Failed to get document." << std::endl;
    
    // if(jResult["_source"].getObject() != jData)
    //     std::cerr << "Oups, something did not work." << std::endl;

    // std::cout << "Great we indexed our first document: " << jResult.pretty() << std::endl;

    // Update document
    // Json::Object jUpdateData;
    // jUpdateData.addMemberByKey("user", "cpp-elasticsearch");
    // if(!es->update("twitter", "tweet", "1", jUpdateData))
    //     std::cerr << "Failed to update document." << std::endl;

    // Search document.
    Json::Object jSearchResult;
    // long resultSize = es->search("twitter", "tweet", "{\"query\":{\"match_all\":{}}}", jSearchResult);
    std::string search_query;
    std::string query="海淀公园在哪里啊请问";
    std::string index = "twitter";
    // sprintf(search_query, , query);
    search_query = "{\"query\":{\"match\":{\"sentence\":\""+query+"\"}}}";
    long resultSize = es->search(index, "tweet", search_query, jSearchResult);
    Json::Object jSearchResult2;
    long resultSize2 = es->search(index, "tweet", search_query, jSearchResult2);
    std::cout << "We found " << resultSize << " result(s):\n" << jSearchResult.pretty() << std::endl;
 
    Json::Array res_array = jSearchResult.getValue("hits").getObject().getValue("hits").getArray();
    std::cout<< res_array<<std::endl;  
    for(auto it = res_array.begin();it!=res_array.end();++it){
        std::cout<<(*it).getObject().getValue("_source").getObject().getValue("sentence")<<std::endl;
        std::cout<<(*it).getObject().getValue("_source").getObject().getValue("sentence_id")<<std::endl;
        std::cout<<(*it).getObject().getValue("_source").getObject().getValue("doc_name")<<std::endl;
    }
    // Delete document. 
    // if(!es->deleteDocument("twitter", "tweet", "1"))
    //     std::cerr << "Failed to delete document." << std::endl;

    // std::cout << "First test is over. Good Bye." << std::endl;

    return EXIT_SUCCESS;
}
