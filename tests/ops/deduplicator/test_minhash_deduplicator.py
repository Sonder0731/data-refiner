import jieba
import pytest

from data_refiner.core import recorder
from data_refiner.ops.deduplicator.minhash_lsh_deduplicator import MinhashLSHDeduplicator


@pytest.fixture(scope="class")
def records(spark):
    test_data = [
        (
            1,
            "Elon Reeve Musk (/ˈiːlɒn/ EE-lon; born June 28, 1971) is an international businessman and entrepreneur known for his leadership of Tesla, SpaceX, X (formerly Twitter), and the Department of Government Efficiency (DOGE). Musk has been the wealthiest person in the world since 2021; as of May 2025, Forbes estimates his net worth to be US$424.7 billion.\n\nBorn to a wealthy family in Pretoria, South Africa, Musk emigrated in 1989 to Canada; he had obtained Canadian citizenship at birth through his Canadian-born mother. He received bachelor's degrees in 1997 from the University of Pennsylvania in Philadelphia, United States, before moving to California to pursue business ventures. In 1995, Musk co-founded the software company Zip2. Following its sale in 1999, he co-founded X.com, an online payment company that later merged to form PayPal, which was acquired by eBay in 2002. That year, Musk also became an American citizen.\n\nIn 2002, Musk founded the space technology company SpaceX, becoming its CEO and chief engineer; the company has since led innovations in reusable rockets and commercial spaceflight. Musk joined the automaker Tesla as an early investor in 2004 and became its CEO and product architect in 2008; it has since become a leader in electric vehicles. In 2015, he co-founded OpenAI to advance artificial intelligence (AI) research but later left; growing discontent with the organization's direction and their leadership in the AI boom in the 2020s led him to establish xAI. In 2022, he acquired the social network Twitter, implementing significant changes and rebranding it as X in 2023. His other businesses include the neurotechnology company Neuralink, which he co-founded in 2016, and the tunneling company the Boring Company, which he founded in 2017.\n\nMusk was the largest donor in the 2024 U.S. presidential election, and is a supporter of global far-right figures, causes, and political parties. In early 2025, he served as senior advisor to United States president Donald Trump and as the de facto head of DOGE. After a public feud with Trump, Musk left the Trump administration and announced he was creating his own political party, the America Party.\n\nMusk's political activities, views, and statements have made him a polarizing figure, especially following the COVID-19 pandemic. He has been criticized for making unscientific and misleading statements, including COVID-19 misinformation and promoting conspiracy theories, and affirming antisemitic, racist, and transphobic comments. His acquisition of Twitter was controversial due to a subsequent increase in hate speech and the spread of misinformation on the service. His role in the second Trump administration attracted public backlash, particularly in response to DOGE.",
            0.1,
        ),
        (
            2,
            "Elon Reeve Musk (/ˈiːlɒn/ EE-lon; born June 28, 1971) is an international businessman and entrepreneur known for his leadership of Tesla, SpaceX, X (formerly Twitter), and the Department of Government Efficiency (DOGE). Musk has been the wealthiest person in the world since 2021; as of May 2025, Forbes estimates his net worth to be US$424.7 billion.\n\nBorn to a wealthy family in Pretoria, South Africa, Musk emigrated in 1989 to Canada; he had obtained Canadian citizenship at birth through his Canadian-born mother. He received bachelor's degrees in 1997 from the University of Pennsylvania in Philadelphia, United States, before moving to California to pursue business ventures. In 1995, Musk co-founded the software company Zip2. Following its sale in 1999, he co-founded X.com, an online payment company that later merged to form PayPal, which was acquired by eBay in 2002. That year, Musk also became an American citizen.\n\nIn 2002, Musk founded the space technology company SpaceX, becoming its CEO and chief engineer; the company has since led innovations in reusable rockets and commercial spaceflight. Musk joined the automaker Tesla as an early investor in 2004 and became its CEO and product architect in 2008; it has since become a leader in electric vehicles. In 2015, he co-founded OpenAI to advance artificial intelligence (AI) research but later left; growing discontent with the organization's direction and their leadership in the AI boom in the 2020s led him to establish xAI. In 2022, he acquired the social network Twitter, implementing significant changes and rebranding it as X in 2023. His other businesses include the neurotechnology company Neuralink, which he co-founded in 2016, and the tunneling company the Boring Company, which he founded in 2017.\n\nMusk was the largest donor in the 2024 U.S. presidential election, and is a supporter of global far-right figures, causes, and political parties. In early 2025, he served as senior advisor to United States president Donald Trump and as the de facto head of DOGE. After a public feud with Trump, Musk left the Trump administration and announced he was creating his own political party, the America Party.",
            0.2,
        ),
        (
            3,
            "Elon Reeve Musk (/ˈiːlɒn/ EE-lon; born June 28, 1971) is an international businessman and entrepreneur known for his leadership of Tesla, SpaceX, X (formerly Twitter), and the Department of Government Efficiency (DOGE). Musk has been the wealthiest person in the world since 2021; as of May 2025, Forbes estimates his net worth to be US$424.7 billion.\n\nBorn to a wealthy family in Pretoria, South Africa, Musk emigrated in 1989 to Canada; he had obtained Canadian citizenship at birth through his Canadian-born mother. He received bachelor's degrees in 1997 from the University of Pennsylvania in Philadelphia, United States, before moving to California to pursue business ventures. In 1995, Musk co-founded the software company Zip2. Following its sale in 1999, he co-founded X.com, an online payment company that later merged to form PayPal, which was acquired by eBay in 2002. That year, Musk also became an American citizen.\n\nIn 2002, Musk founded the space technology company SpaceX, becoming its CEO and chief engineer; the company has since led innovations in reusable rockets and commercial spaceflight. Musk joined the automaker Tesla as an early investor in 2004 and became its CEO and product architect in 2008; it has since become a leader in electric vehicles. In 2015, he co-founded OpenAI to advance artificial intelligence (AI) research but later left; growing discontent with the organization's direction and their leadership in the AI boom in the 2020s led him to establish xAI. In 2022, he acquired the social network Twitter, implementing significant changes and rebranding it as X in 2023. His other businesses include the neurotechnology company Neuralink, which he co-founded in 2016, and the tunneling company the Boring Company, which he founded in 2017.",
            0.3,
        ),
        (
            4,
            "Elon Reeve Musk (/ˈiːlɒn/ EE-lon; born June 28, 1971) is an international businessman and entrepreneur known for his leadership of Tesla, SpaceX, X (formerly Twitter), and the Department of Government Efficiency (DOGE). Musk has been the wealthiest person in the world since 2021; as of May 2025, Forbes estimates his net worth to be US$424.7 billion.",
            0.4,
        ),
        (
            5,
            "北京今天的天气格外晴朗，蓝天白云下，街道上人来人往，公园里孩子们追逐打闹，老人们则悠闲散步。市中心的咖啡馆生意火爆，年轻人三三两两聚在一起聊天。天气的好转让整个城市都显得格外有活力，大家都沉浸在这份温暖与惬意中。",
            0.5,
        ),
        (
            6,
            "北京今天的天气格外晴朗，蓝天白云下，街道上人来人往，公园里孩子们追逐打闹，老人们则悠闲散步。市中心的咖啡馆生意火爆，年轻人三三两两聚在一起聊天。天气的好转让整个城市都显得格外有活力，大家都沉浸在这份温暖与惬意中.",
            0.6,
        ),
        (
            7,
            """第九届会议\n2003年7月28日至8月8日\n牙买加金斯敦\n为来自发展中国家的法"
                "律和技术委员会以及财务委员会成员\n参加委员会会议支付费用的方式\n1. 国际"
                "海底管理局大会第八届会议请秘书长采取一项临时措施，设立一个自愿信托基金，"
                "以便支付来自发展中国家的法律和技术委员会成员以及来自发展中国家的财务委员"
                "会成员参加委员会会议的费用。\n2. 由于秘书长向会员国发出为该信托基金捐款"
                "的请求，已收到三笔捐款，共计10 500美元。 管理局已为基金设立一个单独的账"
                "户。\n3. 管理局第八届会议还决定，由财务委员会审查资助参加这两个委员会会"
                "议的方式，包括审查是否可能从管理局行政预算中提供经费。\n4. 自愿信托基金"
                "迄今收到的捐款数额很小。 这两个委员会成员虽然由缔约国提名，但他们以个人身"
                "份当选。 因此，必须确保这些机构的成员在任期内能够参加会议并且持续不断地履"
                "行职务。 现已注意到，这两个委员会若干成员因旅费和生活津贴费用方面有困难而"
                "未能出席会议。 来自发展中国家成员参加会议的费用估计数见附件，其中比较了经"
                "济舱和公务舱机票价格以及适用于金斯敦的每日生活津贴费用。 从表中可以看出，"
                "根据不同的人数、机舱等级和会议持续时间，每年平均需要捐款120 000美元至"
                "215 000美元。\n5. 为了指导委员会确定提供经费的方式，对某些国际组织的现"
                "行办法作了一次简要调查。 为支付参加会议的旅费和生活费而设立信托基金最相关"
                "的实例是2000年大会为来自发展中国家的大陆架界限委员会成员设立的自愿信托基"
                "金。 目前这一基金正在运作，但现有资源有限。 联合国制定的程序表明，委员会"
                "成员的政府应在规定时间内尽可能提前提出请求。 这种请求按照先到先核可的办法"
                "处理。 提供的机票将是最直接路线的经济舱机票，每日生活津贴将按照联合国费率"
                "提供。 购买机票的所有安排均由联合国秘书处执行。\n6. 虽然已经设立了临时性"
                "的自愿信托基金，但是，对该基金的捐款数额很小，捐款速度很慢。 因此，除了对"
                "信托基金提供自愿捐款的办法之外，建议委员会还可以考虑采用下列办法：\n(a) "
                "从管理局一般行政经费累计利息中拨出一定数额的经费；\n(b) 每年从上一年预算"
                "未动用部分中拨出规定的数额；\n(c) 从先驱投资者基金利息中拨出规定的数额。"
                "\n7. 委员会还不妨建议由管理局秘书处依照行政规则和程序管理该基金，并向财"
                "务委员会提出一份报告。\n附件\n资助来自发展中国家的法律和技术委员会以及财"
                "务\n委员会成员出席会议的指示性费用（美元）\n成员\n机票\n机场\n费用\n金"
                "斯敦每日生活\n津贴\n转机途中每日生活\n7日\n共计\n14日\n经济舱\n公务舱"
                "\n7天=(8天每日生活\n津贴)\n14天= (15天每日生活津贴)\n商务舱\n法律和技"
                "术委员会\n印度尼西亚\n(纽约)\n黎巴嫩\n巴基斯坦\n阿根廷\n喀麦隆\n墨西哥"
                "\n巴西\n塞内加尔\n莫桑比克\n埃及(纽约)\n大韩民国\n印度\n斐济\n智利\n"
                "中国\n纳米比亚\n小计\n财务委员会\n缅甸\n乌干达\n牙买加\n印度(纽约)\n尼"
                "日利亚\n总计\n注：估计费用表表明每年资助每个机构一次会议需要经费120 000"
                "美元至215 000美元(四舍五入)。""",
            0.7,
        ),
        (
            8,
            """第九届会议\n时间：2003年7月28日至8月8日\n牙买加金斯敦\n为来自发展中国家的法"
                "律和技术委员会以及财务委员会成员\n参加委员会会议支付费用的方式\n1. 国际"
                "海底管理局大会第八届会议请秘书长采取一项临时措施，设立一个自愿信托基金，"
                "以便支付来自发展中国家的法律和技术委员会成员以及来自发展中国家的财务委员"
                "会成员参加委员会会议的费用。\n2. 由于秘书长向会员国发出为该信托基金捐款"
                "的请求，已收到三笔捐款，共计10 500美元。 管理局已为基金设立一个单独的账"
                "户。\n3. 管理局第八届会议还决定，由财务委员会审查资助参加这两个委员会会"
                "议的方式，包括审查是否可能从管理局行政预算中提供经费。\n4. 自愿信托基金"
                "迄今收到的捐款数额很小。 这两个委员会成员虽然由缔约国提名，但他们以个人身"
                "份当选。 因此，必须确保这些机构的成员在任期内能够参加会议并且持续不断地履"
                "行职务。 现已注意到，这两个委员会若干成员因旅费和生活津贴费用方面有困难而"
                "未能出席会议。 来自发展中国家成员参加会议的费用估计数见附件，其中比较了经"
                "济舱和公务舱机票价格以及适用于金斯敦的每日生活津贴费用。 从表中可以看出，"
                "根据不同的人数、机舱等级和会议持续时间，每年平均需要捐款120 000美元至"
                "215 000美元。\n5. 为了指导委员会确定提供经费的方式，对某些国际组织的现"
                "行办法作了一次简要调查。 为支付参加会议的旅费和生活费而设立信托基金最相关"
                "的实例是2000年大会为来自发展中国家的大陆架界限委员会成员设立的自愿信托基"
                "金。 目前这一基金正在运作，但现有资源有限。 联合国制定的程序表明，委员会"
                "成员的政府应在规定时间内尽可能提前提出请求。 这种请求按照先到先核可的办法"
                "处理。 提供的机票将是最直接路线的经济舱机票，每日生活津贴将按照联合国费率"
                "提供。 购买机票的所有安排均由联合国秘书处执行。\n6. 虽然已经设立了临时性"
                "的自愿信托基金，但是，对该基金的捐款数额很小，捐款速度很慢。 因此，除了对"
                "信托基金提供自愿捐款的办法之外，建议委员会还可以考虑采用下列办法：\n(a) "
                "从管理局一般行政经费累计利息中拨出一定数额的经费；\n(b) 每年从上一年预算"
                "未动用部分中拨出规定的数额；\n(c) 从先驱投资者基金利息中拨出规定的数额。"
                "\n7. 委员会还不妨建议由管理局秘书处依照行政规则和程序管理该基金，并向财"
                "务委员会提出一份报告。\n附件\n资助来自发展中国家的法律和技术委员会以及财"
                "务\n委员会成员出席会议的指示性费用（美元）\n成员\n机票\n机场\n费用\n金"
                "斯敦每日生活\n津贴\n转机途中每日生活\n7日\n共计\n14日\n经济舱\n公务舱"
                "\n7天=(8天每日生活\n津贴)\n14天= (15天每日生活津贴)\n商务舱\n法律和技"
                "术委员会\n印度尼西亚\n(纽约)\n黎巴嫩\n巴基斯坦\n阿根廷\n喀麦隆\n墨西哥"
                "\n巴西\n塞内加尔\n莫桑比克\n埃及(纽约)\n大韩民国\n印度\n斐济\n智利\n"
                "中国\n纳米比亚\n小计\n财务委员会\n缅甸\n乌干达\n牙买加\n印度(纽约)\n尼"
                "日利亚\n总计\n注：估计费用表表明每年资助每个机构一次会议需要经费120 000"
                "美元至215 000美元(四舍五入)。""",
            0.8,
        ),
    ]

    test_data = [
        (line[0], " ".join(jieba.lcut(line[1])), line[-1]) for index, line in enumerate(test_data)
    ]
    df = spark.createDataFrame(test_data, ["id", "tokens", "score"]).repartition(2)
    df.cache()
    recorder.record("input_df", df)
    return recorder


class TestMinhashDeduplicator:
    def test_mode_dedup_with_normal_comparison_function(self, spark, records):
        mapper = MinhashLSHDeduplicator(
            input_df="input_df",
            output_df="minhash_lsh_deduplicator.df",
            field="tokens",
            index_field="id",
            ev_partitions=1,
            comparison_function="""
            if r1["score"] >= r2["score"]:
                return r1
            else:
                return r2
            """,
            show=True,
        )
        df = mapper.process(spark=spark)
        assert set(row["id"] for row in df.collect()) == {4, 3, 6, 8}

    def test_mode_dup_with_normal_comparison_function(self, spark, records):

        mapper = MinhashLSHDeduplicator(
            input_df="input_df",
            output_df="minhash_lsh_deduplicator.df",
            field="tokens",
            index_field="id",
            checkpoint_dir="",
            mode="dup",
            ev_partitions=1,
            comparison_function="""
            if r1["score"] >= r2["score"]:
                return r1
            else:
                return r2
            """,
            show=True,
        )
        df = mapper.process(spark=spark)
        assert set(row["id"] for row in df.collect()) == {2, 5, 1, 7}

    def test_mode_tag_with_normal_comparison_function(self, spark, records):

        mapper = MinhashLSHDeduplicator(
            input_df="input_df",
            output_df="minhash_lsh_deduplicator.df",
            field="tokens",
            index_field="id",
            mode="dedup_with_dup",
            ev_partitions=1,
            comparison_function="""
            if r1["score"] >= r2["score"]:
                return r1
            else:
                return r2
            """,
            show=True,
        )
        df = mapper.process(spark=spark)
        id_stay_pairs = [(row["id"], row["__stay__"]) for row in df.rdd.collect()]
        assert set(i[0] for i in id_stay_pairs if i[1]) == {4, 8, 3, 6}
        assert set(i[0] for i in id_stay_pairs if not i[1]) == {2, 5, 1, 7}

    def test_mode_dedup_without_comparison_function(self, spark, records):
        mapper = MinhashLSHDeduplicator(
            input_df="input_df",
            output_df="minhash_lsh_deduplicator.df",
            field="tokens",
            index_field="id",
            ev_partitions=1,
            show=True,
        )
        df = mapper.process(spark=spark)
        assert set(row["id"] for row in df.collect()) == {4, 1, 5, 7}
