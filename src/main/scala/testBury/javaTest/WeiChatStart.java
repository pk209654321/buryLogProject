package testBury.javaTest;


import org.apache.commons.lang3.StringUtils;

/**
 * @ClassName WeiChatStart
 * @Description TODO
 * @Author lenovo
 * @Date 2019/8/28 19:37
 **/
/*public class WeiChatStart extends WeChatBot {
    public WeiChatStart(Config config) {
        super(config);
    }

    @Bind(msgType = MsgType.TEXT)
    public void handleText(WeChatMessage message) {
        if (StringUtils.isNotEmpty(message.getName())) {
            this.sendMsg(message.getFromUserName(), "自动回复: " + message.getText());
        }
    }

    public static void main(String[] args) {
        new WeiChatStart(Config.me().autoLogin(true).showTerminal(true)).start();
    }
}*/
