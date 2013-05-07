%%% Copyright (C) 2013 Dmitriy Kataskin
%%%
%%% This file is part of ErlAzure.
%%%
%%% ErlAzure is free software: you can redistribute it and/or modify
%%% it under the terms of the GNU Lesser General Public License as
%%% published by the Free Software Foundation, either version 3 of
%%% the License, or (at your option) any later version.
%%%
%%% ErlAzure is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
%%% GNU Lesser General Public License for more details.
%%%
%%% You should have received a copy of the GNU Lesser General Public
%%% License along with ErlAzure.  If not, see
%%% <http://www.gnu.org/licenses/>.
%%%
%%% Author contact: dmitriy.kataskin@gmail.com

%%% ====================================================================
%%% Queue REST service response parsers.
%%% ====================================================================

-module(erlazure_queue).
-author("dkataskin").

-include("..\\include\\erlazure.hrl").

%% API
-export([parse_queue_list/1, parse_message_list/1, get_request_body/1, get_request_options/1]).

parse_queue_list(Queues) ->
        parse_list(fun parse_queue/1, Queues).

parse_message_list(Messages) ->
        parse_list(fun parse_message/1, Messages).

parse_list(ParseFun, List) ->
        FoldFun = fun(Element, Acc) ->
          [ParseFun(Element) | Acc]
        end,
        lists:reverse(lists:foldl(FoldFun, [], List)).

parse_message({"QueueMessage", _, Elements}) ->
          #queue_message{
            id = erlazure_xml:get_element_text("MessageId", Elements),
            insertion_time = erlazure_xml:get_element_text("InsertionTime", Elements),
            exp_time = erlazure_xml:get_element_text("ExpirationTime", Elements),
            pop_receipt = erlazure_xml:get_element_text("PopReceipt", Elements),
            next_visible = erlazure_xml:get_element_text("TimeNextVisible", Elements),
            dequeue_count = list_to_integer(erlazure_xml:get_element_text("DequeueCount", Elements)),
            text = base64:decode_to_string(erlazure_xml:get_element_text("MessageText", Elements))
          }.

parse_queue({"Queue", _, Elements}) ->
        case lists:keyfind("Metadata", 1, Elements) of
          {"Metadata", _, MetadataElements} ->
            Metadata = parse_metadata(MetadataElements);
          _ -> Metadata = []
        end,

        #queue{
           name = erlazure_xml:get_element_text("Name", Elements),
           url = erlazure_xml:get_element_text("Url", Elements),
           metadata = Metadata
        }.

parse_metadata([]) -> [];
parse_metadata(MetadataElements) ->
        FoldFun = fun({Element, _, Value}, Acc) ->
                    [{Element, lists:flatten(Value)} | Acc]
                  end,
        lists:foldl(FoldFun, [], MetadataElements).

get_request_body(Message) ->
        Data = {'QueueMessage', [], [{'MessageText', [], [base64:encode_to_string(Message)]}]},
        lists:flatten(xmerl:export_simple([Data], xmerl_xml)).

get_request_options(list_queues) -> [prefix, marker, maxresults, include, timeout];
get_request_options(get_queue_acl) -> [timeout];
get_request_options(create_queue) -> [timeout];
get_request_options(delete_queue) -> [timeout];
get_request_options(put_message) -> [visibilitytimeout, messagettl, timeout];
get_request_options(get_messages) -> [numofmessages, visibilitytimeout, timeout];
get_request_options(peek_messages) -> [numofmessages, timeout];
get_request_options(delete_message) -> [timeout];
get_request_options(clear_messages) -> [timeout];
get_request_options(update_message) -> [timeout].