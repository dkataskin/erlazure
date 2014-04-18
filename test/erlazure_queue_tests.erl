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

-module(erlazure_queue_tests).
-compile(export_all).

-author("Dmitry Kataskin").

-include("erlazure.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("xmerl/include/xmerl.hrl").

%% API
-export([]).

%named_test_() ->
%  {setup,
%    fun() -> erlazure:start("<account>", "<key>") end,
%    fun(_) -> ok end,
%    [?_assertMatch({ok, created}, erlazure:create_queue(get_queue_unique_name()))
%    ]
%}.

get_queue_unique_name() ->
                test_utils:append_ticks("TestQueue").

parse_list_queues_response_test() ->
                Response = get_list_queues_sample_response(),
                ParseResult = parse_list_queues_response(Response),
                ?_assertMatch([], ParseResult).

parse_list_queues_response(Response) when is_list(Response) ->
                {ParseResult, _} = xmerl_scan:string(Response),
                parse_list_queues_response(ParseResult).

parse_list_queues_response([], QueueList) ->
                QueueList;

parse_list_queues_response([H|T], QueueList) ->
                QueueList1 = parse_list_queues_response(H, QueueList),
                parse_list_queues_response(T, QueueList1).

parse_list_queues_response(Elem#xmlElement.name='EnumerationResult') ->
                parse_list_queues_response(Elem#xmlElement.content, []).

parse_list_queues_response(Elem#xmlElement.name='Prefix', QueueList) ->
                QueueList.

get_list_queues_sample_response() ->
"<?xml version=""1.0"" encoding=""utf-8""?>
<EnumerationResults ServiceEndpoint=""https://myaccount.queue.core.windows.net/"">
  <Prefix>test prefix</Prefix>
  <Marker>test marker</Marker>
  <MaxResults>133</MaxResults>
  <Queues>
    <Queue>
      <Name>test queue name 1</Name>
      <Metadata>
        <metadata-name>test metadata value 1</metadata-name>
      </Metadata>
    </Queue>
  </Queues>
  <NextMarker />
</EnumerationResults>".