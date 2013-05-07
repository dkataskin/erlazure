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
-author("Dmitriy Kataskin").

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
