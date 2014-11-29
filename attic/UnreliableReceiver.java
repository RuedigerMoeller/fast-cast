package de.ruedigermoeller.fastcast.test;

import org.nustaq.fastcast.remoting.FastCast;
import org.nustaq.fastcast.remoting.Unreliable;

import java.io.IOException;

/**
 * Copyright (c) 2012, Ruediger Moeller. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 *
 * Date: 22.08.13
 * Time: 21:21
 * To change this template use File | Settings | File Templates.
 */
@Unreliable
public class UnreliableReceiver extends UnorderedReceiver {
    public UnreliableReceiver() {
    }

    public static void main( String arg[] ) throws IOException {
        FastCast fc = new FastCast();
        fc.joinCluster("test/unreliablereceiver.yaml", "Bench", null);
    }

}
