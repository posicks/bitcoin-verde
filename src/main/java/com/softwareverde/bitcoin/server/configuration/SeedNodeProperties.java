package com.softwareverde.bitcoin.server.configuration;

import com.softwareverde.logging.Logger;
import com.softwareverde.network.ip.Ip;
import com.softwareverde.network.p2p.node.address.NodeIpAddress;

import java.net.InetAddress;

public class SeedNodeProperties {

    /**
     * Converts SeedNodeProperties to a NodeIpAddress.
     *  This process may incur a DNS lookup.
     *  If the conversion/resolution fails, null is returned.
     */
    public static NodeIpAddress toNodeIpAddress(final SeedNodeProperties seedNodeProperties) {
        final String host = seedNodeProperties.getAddress();
        final String ipAddressString;
        try {
            final InetAddress ipAddress = InetAddress.getByName(host);
            ipAddressString = ipAddress.getHostAddress();
        }
        catch (final Exception exception) {
            Logger.debug("Unable to determine host: " + host);
            return null;
        }

        final Integer port = seedNodeProperties.getPort();
        final Ip ip = Ip.fromString(ipAddressString);

        return new NodeIpAddress(ip, port);
    }

    protected final String _address;
    protected final Integer _port;

    public SeedNodeProperties(final String address, final Integer port) {
        _address = address;
        _port = port;
    }

    public String getAddress() { return _address; }
    public Integer getPort() { return _port; }
}