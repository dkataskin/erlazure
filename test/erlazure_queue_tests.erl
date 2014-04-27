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
                Response = test_utils:read_file("list_queues_response.xml"),
                ParseResult = parse_list_queues_response(Response),

                ?assertMatch({[{queue, "Queue 1", "", [{'metadata-name',"first metadata item"}]}],
                              [{prefix, "Test prefix value"},
                               {marker, "Test marker value"},
                               {max_results, 154},
                               {next_marker, ""}]}, ParseResult).

parse_list_queues_response(Elem, PropListItems) when is_record(Elem, xmlElement) ->
                case Elem#xmlElement.name of
                  'Queues' ->
                    Nodes = erlazure_xml:filter_elements(Elem#xmlElement.content),
                    lists:foldl(fun parse_list_queues_response/2, [], Nodes);

                  'Queue' ->
                    [parse_queue_response(Elem) | PropListItems];

                  _ -> PropListItems
                end.

parse_list_queues_response(Response) when is_list(Response) ->
                {ParseResult, _} = xmerl_scan:string(Response),
                parse_enumeration_result(ParseResult, fun parse_list_queues_response/2).

parse_queue_response(#xmlElement { content = Content}) ->
                Nodes = erlazure_xml:filter_elements(Content),
                lists:foldl(fun parse_queue_response/2, #queue{}, Nodes).

parse_queue_response(Elem, Queue) when is_record(Elem, xmlElement) ->
                case Elem#xmlElement.name of
                  'Name' -> Queue#queue { name = erlazure_xml:parse_str(Elem) };
                  'Metadata' -> Queue#queue { metadata = parse_metadata_response(Elem) };
                  _ -> Queue
                end.

parse_metadata_response(Elem) when is_record(Elem, xmlElement) ->
                Nodes = erlazure_xml:filter_elements(Elem#xmlElement.content),
                lists:foldl(fun parse_metadata_response/2, [], Nodes).

parse_metadata_response(Elem, Items) when is_record(Elem, xmlElement) ->
                [{Elem#xmlElement.name, erlazure_xml:parse_str(Elem)} | Items].

parse_enumeration_common_tokens(Elem, Tokens) when is_record(Elem, xmlElement) ->
                case Elem#xmlElement.name of
                  'Prefix' -> [{prefix, erlazure_xml:parse_str(Elem)} | Tokens];
                  'Marker' -> [{marker, erlazure_xml:parse_str(Elem)} | Tokens];
                  'MaxResults' -> [{max_results, erlazure_xml:parse_int(Elem)} | Tokens];
                  'NextMarker' -> [{next_marker, erlazure_xml:parse_str(Elem)} | Tokens];
                  _ -> Tokens
                end.

parse_enumeration_result(Elem, ParseFun) when is_record(Elem, xmlElement) ->
                case Elem#xmlElement.name of
                  'EnumerationResults' ->
                    Nodes = erlazure_xml:filter_elements(Elem#xmlElement.content),
                    CommonTokens = lists:foldl(fun parse_enumeration_common_tokens/2, [], Nodes),
                    Items = lists:foldl(ParseFun, [], Nodes),
                    {lists:reverse(Items), lists:reverse(CommonTokens)};

                  _ -> {error, bad_response}
                end.