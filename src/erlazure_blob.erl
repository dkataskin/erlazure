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
-module(erlazure_blob).
-author("Dmitriy Kataskin").

-include("..\\include\\erlazure.hrl").

%% API
-export([get_request_options/1, parse_container_list/1, parse_blob_list/1, parse_blob/1,
          str_to_blob_type/1, blob_type_to_str/1, get_request_body/1, parse_block_list/1]).

parse_container_list(Containers) ->
          erlazure_xml:parse_list(fun parse_container/1, Containers).

parse_blob_list(Blobs) ->
          erlazure_xml:parse_list(fun parse_blob/1, Blobs).

parse_block_list(Blocks) ->
          io:format("~p~n", [Blocks]),
          Committed = parse_block_list("CommittedBlocks", committed, Blocks),
          Uncommitted = parse_block_list("UncommittedBlocks", uncommitted, Blocks),
          Committed ++ Uncommitted.

parse_block_list(NodeName, BlockType, Blocks) ->
          case lists:keyfind(NodeName, 1, Blocks) of
                {NodeName, _, Elements} ->
                        ParseFun = fun(Element) -> parse_block(Element, BlockType) end,
                        erlazure_xml:parse_list(ParseFun, Elements);
                false -> []
          end.

parse_container({"Container", _, Elements}) ->
          #blob_container{
            name = erlazure_xml:get_element_text("Name", Elements),
            url = erlazure_xml:get_element_text("Url", Elements),
            metadata = erlazure_xml:parse_metadata(Elements)
          }.

parse_blob({"Blob", _, Elements}) ->

          {"Properties", _, Properties} = lists:keyfind("Properties", 1, Elements),

          #cloud_blob{
              name = erlazure_xml:get_element_text("Name", Elements),
              url = erlazure_xml:get_element_text("Url", Elements),
              last_modified = erlazure_xml:get_element_text("Last-Modified", Properties),
              etag = erlazure_xml:get_element_text("ETag", Properties),
              content_length = list_to_integer(erlazure_xml:get_element_text("Content-Length", Properties)),
              content_type = erlazure_xml:get_element_text("Content-Type", Properties),
              content_encoding = erlazure_xml:get_element_text("Content-Encoding", Properties),
              content_language = erlazure_xml:get_element_text("Content-Language", Properties),
              content_md5 = erlazure_xml:get_element_text("Content-MD5", Properties),
              cache_control = erlazure_xml:get_element_text("Cache-Control", Properties),
              type = str_to_blob_type(erlazure_xml:get_element_text("BlobType", Properties)),
              copy = parse_copy_state(Properties),
              metadata = erlazure_xml:parse_metadata(Elements)
          }.

parse_block({"Block", _, Elements}, Type) ->
          #blob_block{
              id = base64:decode_to_string(erlazure_xml:get_element_text("Name", Elements)),
              size = list_to_integer(erlazure_xml:get_element_text("Size", Elements)),
              type = Type
          }.

parse_copy_state(Properties) ->
          case lists:keyfind("CopyId", 1, Properties) of
            {"CopyId", _, _} ->
                #blob_copy_state{
                      id = erlazure_xml:get_element_text("CopyId", Properties),
                      status = list_to_atom(erlazure_xml:get_element_text("CopyStatus", Properties)),
                      source = erlazure_xml:get_element_text("CopySource", Properties),
                      progress = erlazure_xml:get_element_text("CopyProgress", Properties),
                      completion_time = erlazure_xml:get_element_text("CopyCompletionTime", Properties),
                      status_description = erlazure_xml:get_element_text("CopyStatusDescription", Properties)
                };
          false ->
            undefined
          end.

str_to_blob_type("BlockBlob") -> block_blob;
str_to_blob_type("PageBlob") -> page_blob.

blob_type_to_str(block_blob) -> "BlockBlob";
blob_type_to_str(page_blob) -> "PageBlob".

str_to_block_type("Uncommitted") -> uncommitted;
str_to_block_type("Committed") -> committed;
str_to_block_type("Latest") -> latest.

block_type_to_str(uncommitted) -> "Uncommitted";
block_type_to_str(committed) -> "Committed";
block_type_to_str(latest) -> "Latest".

block_type_to_node(uncommitted) -> 'Uncommitted';
block_type_to_node(committed) -> 'Committed';
block_type_to_node(latest) -> 'Latest'.

get_request_body(BlockRefs) ->
          FoldFun = fun(BlockRef=#blob_block_ref{}, Acc) ->
                        [{block_type_to_node(BlockRef#blob_block_ref.type),
                          [],
                          [base64:encode_to_string(BlockRef#blob_block_ref.id)]} | Acc]
                    end,
          Data = {'BlockList', [], lists:reverse(lists:foldl(FoldFun, [], BlockRefs))},
          lists:flatten(xmerl:export_simple([Data], xmerl_xml)).

get_request_options(list_containers) -> [prefix, marker, maxresults, include, timeout];
get_request_options(create_container) -> [timeout];
get_request_options(delete_container) -> [timeout];
get_request_options(lease_container) -> [timeout];
get_request_options(list_blobs) -> [prefix, marker, maxresults, include, timeout];
get_request_options(put_blob) -> [timeout, content_type, content_encoding, content_language, content_md5];
get_request_options(get_blob) -> [timeout, snapshot];
get_request_options(snapshot_blob) -> [timeout];
get_request_options(copy_blob) -> [timeout];
get_request_options(delete_blob) -> [timeout, snapshot];
get_request_options(put_block) -> [timeout];
get_request_options(put_block_list) -> [timeout];
get_request_options(get_block_list) -> [timeout, snapshot, blocklisttype].
