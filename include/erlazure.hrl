%% Copyright
-author("dkataskin").

-record(queue, {name="", url="", metadata=[]}).
-record(access_policy, {start="", expiry="", permission=""}).
-record(signed_id, {id="", access_policy=#access_policy{}}).
-record(queue_message, {id="",
                        insertion_time="",
                        exp_time="",
                        pop_receipt="",
                        next_visible="",
                        dequeue_count=0,
                        text=""}).
