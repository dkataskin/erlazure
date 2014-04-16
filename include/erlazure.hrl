%% Copyright (c) 2013 - 2014, Dmitry Kataskin
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions are met:
%%
%% * Redistributions of source code must retain the above copyright notice,
%% this list of conditions and the following disclaimer.
%% * Redistributions in binary form must reproduce the above copyright
%% notice, this list of conditions and the following disclaimer in the
%% documentation and/or other materials provided with the distribution.
%% * Neither the name of  nor the names of its contributors may be used to
%% endorse or promote products derived from this software without specific
%% prior written permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
%% ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
%% LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
%% CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
%% SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
%% INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
%% CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
%% ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
%% POSSIBILITY OF SUCH DAMAGE.

-author("Dmitry Kataskin").

-define(http_ok, 200).
-define(http_created, 201).
-define(http_accepted, 202).
-define(http_no_content, 204).

-define(blob_service, blob).
-define(table_service, table).
-define(queue_service, queue).

-define(queue_service_ver, "2012-02-12").
-define(blob_service_ver, "2012-02-12").

-record(service_context, {service, api_version, account, key}).

-record(req_context, {method = get,
                          address,
                          path = "",
                          parameters = [],
                          content_type = "application/xml",
                          content_length = 0,
                          body = "",
                          headers = []}).

-record(param_spec, {id,
                     type,
                     name,
                     parse_fun = fun(Value) -> lists:flatten(io_lib:format("~p", [Value])) end}).

% Queue
-record(queue, {name="",
                url="",
                metadata=[]}).

-record(access_policy, {start="",
                        expiry="",
                        permission=""}).

-record(signed_id, {id="",
                    access_policy=#access_policy{}}).

-record(queue_message, {id="",
                        insertion_time="",
                        exp_time="",
                        pop_receipt="",
                        next_visible="",
                        dequeue_count=0,
                        text=""}).

% Blob
-record(blob_container, {name="",
                         url="",
                         properties=[],
                         metadata=[]}).

-record(blob_lease, {id="",
                     status,
                     state,
                     duration}).

-record(blob_copy_state, {id="",
                          status,
                          source="",
                          progress="",
                          completion_time="",
                          status_description=""}).

-record(cloud_blob, {name="",
                     snapshot="",
                     url="",
                     last_modified="",
                     etag="",
                     content_length=0,
                     content_type="",
                     content_encoding="",
                     content_language="",
                     content_md5="",
                     cache_control="",
                     type,
                     copy,
                     metadata=[]}).

-record(blob_block_ref, {id="", type=undefined}).

-record(blob_block, {id="",
                     type=undefined,
                     size=0}).