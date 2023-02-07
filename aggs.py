from hashlib import sha256
from elasticsearch import Elasticsearch
import json


answer = '9a6f8f6742f7403b55cdb9dd420f7448bbe88f1d3b30e60d898cd92b9301d89b'

def check(response):
    value = response.get('aggregations', {}).get('statistics', {}).get('value', {})
    j_str = json.dumps(value, sort_keys=True)
    hashed = sha256(j_str.encode('utf8')).hexdigest()
    status = True if hashed == answer else False
    return status

def get_gpsid_list():
    gpsid_list = {}
    with open("gpsid_list.json", "r") as f:
        gpsid_list = json.load(f)
    return gpsid_list

query = {
    "match_all": {
    }
}

aggs = {
    "statistics": {
        "scripted_metric": {
            "params":{
                "gpsid_list" : get_gpsid_list()
            },
            "init_script": """
                state.transactions = new HashMap(); 
                state.ts2 = new HashMap(); 
            """,
            "map_script": """
                String name = doc.name.value;
                List info = doc.user_info;
                String lv1 = name.substring(0, name.indexOf("|"));
                if(!state.transactions.containsKey(lv1)){
                    state.transactions[lv1] = new HashMap();
                }
                for(int i=0; i<info.length; i++){
                    String gpsid = info[i].substring(0, info[i].indexOf(","));
                    int click = Integer.parseInt(info[i].substring(info[i].indexOf(",")+1));
                    state.transactions[lv1].put(gpsid, state.transactions[lv1].getOrDefault(gpsid, 0)+click);
                }
            """,
            "combine_script": """
                return state.transactions;
            """,
            "reduce_script": """
                HashMap result = new HashMap();
                HashMap user_list = new HashMap();
                while(states.size()>0){
                    Map data_buffer = states.remove(0);
                    for(lv1 in data_buffer.keySet()){
                        int  t_aud = 0, p_aud = 0, t_sum = 0, p_sum = 0;
                        HashMap mix = new HashMap();
                        for(gpsid in data_buffer[lv1].keySet()){
                            t_sum += data_buffer[lv1].get(gpsid);
                            if(params.gpsid_list.contains(gpsid)){
                                p_aud += data_buffer[lv1].get(gpsid);
                            }
                            if(!user_list.containsKey(lv1)){
                                user_list.put(lv1, new ArrayList());
                            }
                            if(!user_list.get(lv1).contains(gpsid)){
                                user_list.get(lv1).add(gpsid);
                                p_sum ++; 
                                if(params.gpsid_list.contains(gpsid)){
                                    t_aud ++;
                                }
                            }             
                        }
                        if(!result.containsKey(lv1)){
                            mix.put("受眾人數", t_aud);
                            mix.put("受眾次數", p_aud);
                            mix.put("所有人數", p_sum);
                            mix.put("所有次數", t_sum);
                            result[lv1] = mix;
                        }
                        else{
                            result[lv1]["受眾人數"]+=t_aud;
                            result[lv1]["受眾次數"]+=p_aud;
                            result[lv1]["所有人數"]+=p_sum;
                            result[lv1]["所有次數"]+=t_sum;
                        }
                    }
                }
                return result;
            """
        }
    }
}



if __name__ == "__main__":
    es = Elasticsearch(hosts='http://localhost:9200', timeout=5000)
    response = es.search(index="anal_product_2019-09", query=query, aggs=aggs, size=0)
    
    print("Correct: ", check(response))
    