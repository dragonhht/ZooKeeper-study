package hht.dragon.pub.sub;

import lombok.*;

/**
 * 配置信息类.
 *
 * @author: huang
 * @Date: 2019-5-15
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class PropertiesPojo {
    private String className;
    private String value;
    private int size;
}
