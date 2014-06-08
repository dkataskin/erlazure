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

-module(erlazure_utils_tests).
-author("Dmitry Kataskin").

-compile(export_all).

-include("erlazure.hrl").
-include_lib("eunit/include/eunit.hrl").

format_date_only_test() ->
                Date = {2014, 10, 22},
                Fmt = erlazure_utils:iso_8601_fmt(Date),
                ?assertMatch("2014-10-22", Fmt).

format_date_time_hhmmss_test() ->
                DateTime = {{2014, 10, 22}, {22, 15}},
                Fmt = erlazure_utils:iso_8601_fmt(DateTime),
                ?assertMatch("2014-10-22T22:15Z", Fmt).
format_date_time_hhmm_test() ->
                DateTime = {{2014, 10, 22}, {22, 15, 10}},
                Fmt = erlazure_utils:iso_8601_fmt(DateTime),
                ?assertMatch("2014-10-22T22:15:10Z", Fmt).
