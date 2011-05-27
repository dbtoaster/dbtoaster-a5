/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package codecs;

import java.nio.charset.Charset;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.string.StringDecoder;

/**
 *
 * @author kunal
 */
public class ModStringDecoder extends StringDecoder{
    String pending = "";
    @Override
    protected Object decode(
            ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
        if (!(msg instanceof ChannelBuffer)) {
            return msg;
        }
        else{
            String message =  ((ChannelBuffer) msg).toString(Charset.defaultCharset());
            message = pending+message;
            int index = message.lastIndexOf("\n");
            String toRet = message.substring(0, index);
            pending = message.substring(index+1);
            return toRet;
        }
    }
    
}
