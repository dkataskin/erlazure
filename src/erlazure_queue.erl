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


%%% ====================================================================
%%% Queue REST service response parsers.
%%% ====================================================================

-module(erlazure_queue).
-author("Dmitry Kataskin").

-include("erlazure.hrl").
-include_lib("xmerl/include/xmerl.hrl").

%% API
-export([parse_queue_list/1, parse_message_list/1, get_request_body/1, get_request_param_specs/0]).

parse_message_list(Messages) ->
          erlazure_xml:parse_list(fun parse_message/1, Messages).

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

parse_queue_list(Elem, PropListItems) when is_record(Elem, xmlElement) ->
          case Elem#xmlElement.name of
            'Queues' ->
              Nodes = erlazure_xml:filter_elements(Elem#xmlElement.content),
              lists:foldl(fun parse_queue_list/2, [], Nodes);
            'Queue' -> [parse_queue_response(Elem) | PropListItems];
            _ -> PropListItems
          end.

parse_queue_list(Response) when is_list(Response) ->
          {ParseResult, _} = xmerl_scan:string(Response),
          erlazure_xml:parse_enumeration(ParseResult, fun parse_queue_list/2).

parse_queue_response(#xmlElement { content = Content}) ->
          Nodes = erlazure_xml:filter_elements(Content),
          lists:foldl(fun parse_queue_response/2, #queue{}, Nodes).

parse_queue_response(Elem, Queue) when is_record(Elem, xmlElement) ->
          case Elem#xmlElement.name of
            'Name' -> Queue#queue { name = erlazure_xml:parse_str(Elem) };
            'Url' -> Queue#queue { url = erlazure_xml:parse_str(Elem) };
            'Metadata' -> Queue#queue { metadata = erlazure_xml:parse_metadata(Elem) };
            _ -> Queue
          end.

get_request_body(Message) ->
          Data = {'QueueMessage', [], [{'MessageText', [], [base64:encode_to_string(Message)]}]},
          lists:flatten(xmerl:export_simple([Data], xmerl_xml)).

get_request_param_specs() ->
          [#param_spec{ id = num_of_messages, type = uri, name = "numofmessages" },
           #param_spec{ id = pop_receipt, type = uri, name = "popreceipt" },
           #param_spec{ id = peek_only, type = uri, name = "peekonly" },
           #param_spec{ id = message_ttl, type = uri, name = "messagettl" },
           #param_spec{ id = message_visibility_timeout, type = uri, name = "visibilitytimeout" }].