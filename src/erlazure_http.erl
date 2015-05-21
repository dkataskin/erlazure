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
%% * Neither the name of erlazure nor the names of its contributors may be used to
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

-module(erlazure_http).
-author("Dmitry Kataskin").

-include("../include/erlazure.hrl").

%% API
-export([verb_to_str/1, get_content_length/1, create_request/2]).

verb_to_str(get) -> "GET";
verb_to_str(put) -> "PUT";
verb_to_str(post) -> "POST";
verb_to_str(head) -> "HEAD";
verb_to_str(delete) -> "DELETE".

create_request(ReqContext = #req_context{ method = get }, Headers) ->
        {construct_url(ReqContext), Headers};

create_request(ReqContext = #req_context{ method = delete }, Headers) ->
        {construct_url(ReqContext), Headers};

create_request(ReqContext = #req_context{}, Headers) ->
        {construct_url(ReqContext),
         Headers,
         ReqContext#req_context.content_type,
         ReqContext#req_context.body}.

construct_url(ReqContext = #req_context{}) ->
        FoldFun = fun({ParamName, ParamValue}, Acc) ->
          if Acc =:= "" ->
            lists:concat(["?", ParamName, "=", ParamValue]);

            true ->
              lists:concat([Acc, "&", ParamName, "=", ParamValue])
          end
        end,

        ReqContext#req_context.address ++
        ReqContext#req_context.path ++
        lists:foldl(FoldFun, "", ReqContext#req_context.parameters).

get_content_length(Content) when is_list(Content) ->
        lists:flatlength(Content);

get_content_length(Content) when is_binary(Content) ->
        byte_size(Content).