import pickle
import time
import redis
from redis import ConnectionPool

# 连接池做成单例的   可以放在启动文件里， import进来
redis_pool = ConnectionPool(host="localhost", port=6379, db=0)


class MyRedis(object):
    """
    redis数据库操作  方法可以直接用类名调用
    """

    @staticmethod
    def _get_r():
        redis_instance = redis.StrictRedis(connection_pool=redis_pool)
        return redis_instance

    @classmethod
    def set(cls, key, value, expire=None):
        """
        写入键值对
        """
        # 判断是否有过期时间，没有就设置默认值
        if expire:
            expire_in_seconds = expire
        else:
            expire_in_seconds = 3600
        r = cls._get_r()
        # value = pickle.dumps(value)
        r.set(key, value, ex=expire_in_seconds)

    @classmethod
    def get(cls, key):
        """
        读取键值对内容
        """
        r = cls._get_r()
        value = r.get(key)
        return value.decode('utf-8') if value else value
        # return pickle.loads(value)

    @classmethod
    def lock_get(cls, key):
        """
        加锁读取键值对内容  多线程写入时等其他线程写入再取
        """
        r = cls._get_r()
        value = r.get(key)
        if value:
            return value.decode('utf-8')
        else:
            flag_key = key + "_" + "flag"
            while True:
                flag = cls.get_eval(flag_key)
                if flag:
                    print(key, "已经有其他线程在写入")
                    time.sleep(0.5)
                    value = r.get(key)
                    if value:
                        print(key, "等待查询成功")
                        return value.decode('utf-8')
                else:
                    cls.set(flag_key, True, expire=120)
                    return

    @classmethod
    def get_eval(cls, key):
        """
        读取键值对内容
        """
        r = cls._get_r()
        value = r.get(key)
        val = value.decode('utf-8') if value else value
        try:
            re_val = eval(val)
            return re_val
        except Exception as e:
            return val

    @classmethod
    def hset(cls, name, key, value):
        """
        写入hash表
        """
        r = cls._get_r()
        r.hset(name, key, value)

    @classmethod
    def hmset(cls, key, *value):
        """
        读取指定hash表的所有给定字段的值
        """
        r = cls._get_r()
        value = r.hmset(key, *value)
        return value

    @classmethod
    def hget(cls, name, key):
        """
        读取指定hash表的键值
        """
        r = cls._get_r()
        value = r.hget(name, key)
        return value.decode('utf-8') if value else value

    @classmethod
    def hgetall(cls, name):
        """
        获取指定hash表所有的值
        """
        r = cls._get_r()
        return r.hgetall(name)

    @classmethod
    def keys(cls, pattern='*'):
        """
        获取匹配到的键列表
        """
        r = cls._get_r()
        return r.keys(pattern)

    @classmethod
    def delete(cls, *names):
        """
        删除一个或者多个
        """
        r = cls._get_r()
        r.delete(*names)

    @classmethod
    def delete_start_with(cls, key_prefix):
        """
        根据前缀删除key
        """
        pattern = key_prefix + "*"
        my_keys = cls.keys(pattern)
        for k in my_keys:
            cls.delete(k)

    @classmethod
    def hdel(cls, name, key):
        """
        删除指定hash表的键值
        """
        r = cls._get_r()
        r.hdel(name, key)

    @classmethod
    def exists(cls, name):
        """
        判断redis中是否存在某个key
        """
        r = cls._get_r()
        return r.execute_command('EXISTS', name)

    @classmethod
    def expire(cls, name, expire=None):
        """
        设置过期时间
        """
        if expire:
            expire_in_seconds = expire
        else:
            expire_in_seconds = 3600
        r = cls._get_r()
        r.expire(name, expire_in_seconds)


if __name__ == '__main__':
    key = "symbol"
    value = "IF"
    MyRedis.set(key, value)
    res_value = MyRedis.lock_get(key)
    print(res_value)
