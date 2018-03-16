from data_scrapy.items import JobItem
from data_scrapy.base_spider import BaseSpider
from scrapy import Request
from scrapy.selector import Selector
from scrapy.exceptions import CloseSpider
import pickle
import logging


class Job51Spider(BaseSpider):
    name = 'job51'
    allowed_domains = ['search.51job.com', 'jobs.51job.com']

    def start_requests(self):
        start_urls = []
        city_codes = {'白银': '270800', '绥化': '220400', '石河子': '310800', '迪庆': '252000', '山南': '300500', '吉安': '130900',
                      '泉州': '110400', '衡水': '161200', '攀枝花': '091000', '定西': '271100', '张掖': '270900', '邯郸': '160700',
                      '东营': '121000', '伊犁': '310500', '天津': '050000', '雄安新区': '160100', '盘锦': '231300', '日喀则': '300300',
                      '平顶山': '171000', '白沙': '101800', '乌海': '281000', '晋城': '210700', '黑河': '221200', '包头': '280400',
                      '泰州': '071800', '驻马店': '171400', '潍坊': '120500', '邓州': '172000', '郑州': '170200', '日照': '121200',
                      '临高': '101400', '通辽': '280700', '汉中': '200900', '晋中': '211000', '上饶': '131200', '怒江': '251900',
                      '肇庆': '031800', '甘孜': '092100', '运城': '210300', '唐山': '160500', '海西': '320400', '黄山': '151000',
                      '遂宁': '091500', '六安': '151200', '荆州': '180700', '上海': '020000', '淄博': '120700', '成都': '090200',
                      '洋浦经济开发区': '100400', '临沧': '251800', '鹤壁': '171700', '江门': '031500', '保亭': '101700',
                      '商丘': '171300', '仙桃': '181400', '阜新': '231500', '牡丹江': '220700', '塔城': '311500', '合肥': '150200',
                      '延安': '200600', '乌鲁木齐': '310200', '杭州': '080200', '呼和浩特': '280200', '汕头': '030400',
                      '酒泉': '270500', '宜昌': '180300', '南昌': '130200', '阳泉': '210800', '云浮': '032900', '枣庄': '121600',
                      '齐齐哈尔': '220600', '铜陵': '150800', '雅安': '091800', '长沙': '190200', '三门峡': '171800', '昌都': '300600',
                      '张家口': '160900', '德宏': '251600', '许昌': '171100', '营口': '230500', '果洛': '320800', '楚雄': '251700',
                      '白山': '240900', '保山': '251200', '娄底': '191200', '昌吉': '311200', '武汉': '180200', '常德': '190700',
                      '图木舒克': '311100', '滁州': '150900', '葫芦岛': '230900', '黄石': '180400', '崇左': '141400', '大连': '230300',
                      '贺州': '141500', '佳木斯': '220800', '广州': '030200', '开平': '032700', '内江': '090900', '辽阳': '231100',
                      '三沙': '101500', '兴安盟': '281300', '孝感': '180900', '沧州': '160800', '十堰': '180600', '宿迁': '072000',
                      '怀化': '191100', '马鞍山': '150500', '新余': '130600', '毕节': '260700', '大兴安岭': '221400', '忻州': '211100',
                      '昆明': '250200', '黔西南': '260800', '南通': '070900', '铜川': '200500', '鄂尔多斯': '280800', '宿州': '151600',
                      '克拉玛依': '310300', '四平': '240600', '长春': '240200', '丹阳': '072100', '琼海': '100600', '中山': '030700',
                      '延边': '241100', '龙岩': '111000', '五指山': '101000', '北京': '010000', '鄂州': '181000', '义乌': '081400',
                      '桂林': '140300', '湘西': '191500', '汕尾': '032400', '西宁': '320200', '固原': '290600', '潮州': '032000',
                      '吕梁': '211200', '河源': '032100', '池州': '151500', '常熟': '070700', '淮南': '151100', '凉山': '092300',
                      '廊坊': '160300', '景德镇': '130400', '临沂': '120800', '郴州': '190900', '保定': '160400', '嘉兴': '080700',
                      '鹤岗': '221000', '文山': '251400', '莱芜': '121800', '鹰潭': '130700', '徐州': '071100', '周口': '170800',
                      '滨州': '121500', '常州': '070500', '重庆': '060000', '芜湖': '150300', '益阳': '190800', '吴忠': '290300',
                      '辽源': '240400', '双鸭山': '221100', '衡阳': '190500', '商洛': '201100', '济南': '120200', '杨凌': '201200',
                      '黄冈': '181100', '青岛': '120300', '石嘴山': '290500', '亳州': '151800', '太原': '210200', '来宾': '141300',
                      '六盘水': '260400', '宣城': '151400', '盐城': '071300', '金昌': '270300', '儋州': '100800', '梅州': '032600',
                      '和田': '311600', '随州': '181200', '阜阳': '150700', '资阳': '091400', '襄阳': '180500', '海宁': '081600',
                      '赤峰': '280300', '武威': '270700', '嘉峪关': '270400', '贵阳': '260200', '承德': '161000', '大庆': '220500',
                      '东莞': '030800', '泸州': '090500', '德州': '121300', '石家庄': '160200', '北海': '140500', '达州': '091700',
                      '泰安': '121100', '珠海': '030500', '咸宁': '181300', '绍兴': '080500', '昌江': '101900', '湘潭': '190400',
                      '安顺': '260500', '呼伦贝尔': '281100', '朔州': '210900', '洛阳': '170300', '福州': '110200', '中卫': '290400',
                      '菏泽': '121400', '张家界': '191400', '乌兰察布': '281200', '拉萨': '300200', '天水': '270600', '揭阳': '032200',
                      '邢台': '161100', '连云港': '071200', '巴彦淖尔': '280900', '澄迈': '101300', '株洲': '190300', '甘南': '271500',
                      '百色': '141100', '南京': '070200', '红河州': '251000', '聊城': '121700', '海东': '320300', '漳州': '110500',
                      '庆阳': '271300', '文昌': '100500', '克孜勒苏柯尔克孜': '311700', '黔南': '261000', '海北': '320500',
                      '济源': '171900', '惠州': '030300', '厦门': '110300', '防城港': '140800', '太仓': '071600', '焦作': '170500',
                      '兰州': '270200', '眉山': '091200', '屯昌': '101200', '昭通': '251300', '吐鲁番': '311400', '莆田': '110600',
                      '伊春': '220300', '淮安': '071900', '丹东': '230800', '宁德': '110900', '燕郊开发区': '161300', '丽水': '081000',
                      '临夏': '271400', '大理': '250500', '定安': '101100', '温州': '080400', '延吉': '240800', '岳阳': '190600',
                      '南宁': '140200', '恩施': '181800', '新乡': '170700', '邵阳': '191000', '东方': '100900', '漯河': '171500',
                      '金华': '080600', '博尔塔拉': '311900', '本溪': '231000', '西昌': '091900', '德阳': '090600', '玉林': '140600',
                      '万宁': '100700', '沈阳': '230200', '松原': '240700', '张家港': '071400', '抚顺': '230600', '深圳': '040000',
                      '湖州': '080900', '丽江': '250600', '朝阳': '231400', '榆林': '200800', '泰兴': '072300', '七台河': '221300',
                      '信阳': '171200', '普洱': '251100', '开封': '170400', '昆山': '070600', '三亚': '100300', '那曲': '300700',
                      '曲靖': '250300', '安阳': '170900', '安康': '201000', '乐东': '102000', '阿拉尔': '310900', '咸阳': '200300',
                      '西安': '200200', '安庆': '150400', '威海': '120600', '济宁': '120900', '神农架': '181700', '清远': '031900',
                      '自贡': '090800', '喀什地区': '310400', '柳州': '140400', '陵水': '102100', '五家渠': '311000', '韶关': '031400',
                      '梧州': '140700', '西双版纳': '251500', '陇南': '271200', '阿勒泰': '311300', '玉树': '320900', '钦州': '140900',
                      '烟台': '120400', '鞍山': '230400', '琼中': '101600', '宝鸡': '200400', '锡林郭勒盟': '281400', '长治': '210600',
                      '鸡西': '220900', '遵义': '260300', '天门': '181600', '哈密': '310700', '湛江': '031700', '茂名': '032300',
                      '阿克苏': '310600', '蚌埠': '150600', '舟山': '081100', '平凉': '271000', '九江': '130300', '哈尔滨': '220200',
                      '南充': '091100', '巴音郭楞': '311800', '抚州': '131100', '靖江': '072500', '吉林': '240300', '海南': '320700',
                      '广安': '091300', '阳江': '032800', '苏州': '070300', '海口': '100200', '永州': '191300', '阿拉善盟': '281500',
                      '淮北': '151700', '乐山': '090400', '黔东南': '260900', '宜宾': '090700', '濮阳': '171600', '镇江': '071000',
                      '贵港': '141000', '荆门': '180800', '萍乡': '130500', '南阳': '170600', '渭南': '200700', '三明': '110700',
                      '阿坝': '092200', '南平': '110800', '阿里': '300800', '临汾': '210500', '林芝': '300400', '秦皇岛': '160600',
                      '台州': '080800', '河池': '141200', '银川': '290200', '白城': '241000', '潜江': '181500', '玉溪': '250400',
                      '铁岭': '231200', '宜春': '131000', '锦州': '230700', '大同': '210400', '无锡': '070400', '绵阳': '090300',
                      '广元': '091600', '扬州': '070800', '佛山': '030600', '赣州': '130800', '衢州': '081200', '巴中': '092000',
                      '通化': '240500', '黄南': '320600', '珠三角': '01', '铜仁': '260600', '宁波': '080300'}
        code_list = []
        for city in self.city_list:
            temp = city_codes.get(city, '')
            if temp:
                code_list.append(temp)
        for code in code_list:
            for word in self.INSPECT_WORDS:
                start_urls.append('http://search.51job.com/jobsearch/search_result.php?'
                                  'jobarea={}&keyword={}'.format(code, word))
        for url in start_urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response):
        sel = Selector(response)
        items = sel.css('div .dw_table div.el')[1:]
        count = 3
        for item in items:
            job_name = item.css('p.t1>span>a::text').extract()[0].strip()
            job_company_name = item.css('span.t2>a::text').extract()[0]
            flag = False
            for word in self.FILTER_WORDS:
                if word in job_name or word in job_company_name:
                    url = item.css('p.t1>span>a').xpath('@href').extract()[0]
                    count = 3
                    flag = True
                    yield Request(url=url, callback=self.parse_item)
                    break
            if not flag:
                logging.log(logging.INFO, "There is no key word in the position's name--{}".format(job_name))
                count -= 1
                if not count:
                    logging.log(logging.INFO, "No key words for three times,Stop!")
                return
        next_page = sel.css('li.bk>a')
        if next_page:
            if len(next_page) == 1:
                link = next_page.xpath('@href').extract()[0]
            else:
                link = next_page.xpath('@href').extract()[1]
            yield Request(url=link, callback=self.parse)

    def parse_item(self, response):
        if response.status != 200:
            raise CloseSpider(reason='This spider has been blocked')
        sel = Selector(response)
        item = JobItem()
        item['url'] = response.url
        item['from_website'] = self.name
        head_info = sel.css('div.cn')
        job_name = head_info.css('h1::text').extract()[0]
        item['name'] = job_name
        location = head_info.css('span.lname::text').extract()[0]
        item['location'] = location.split('-')[0]
        item['company_name'] = head_info.css('p.cname>a::text').extract()[0]
        temp = [each.strip() for each in head_info.css('p.msg::text').extract()[0].split('|')]
        if len(temp) == 3:
            item['company_type'], item['company_scale'], item['company_industry'] = temp
        elif len(temp) == 2:
            if '公司' in temp[0]:
                item['company_type'], item['company_industry'] = temp
                item['company_scale'] = 'Unknown'
            else:
                item['company_scale'], item['company_industry'] = temp
                item['company_type'] = 'Unknown'
        elif len(temp) == 1:
            item['company_type'] = temp[0]
            item['company_scale'] = item['company_industry'] = 'Unknown'
        else:
            raise CloseSpider('A bug was found,url is %s' % response.url)
        try:
            item['salary'] = head_info.css('div.cn strong::text').extract()[0]
        except IndexError:
            item['salary'] = '面议'
        middle_info = sel.css('div.t1')
        requirements = middle_info.css('span.sp4::text').extract()
        assert len(requirements) == 4 or len(requirements) == 3
        if len(requirements) == 4:
            item['exp'], item['edu'], item['available_number'], item['pub_date'] = requirements
        elif len(requirements) == 3:
            item['exp'], item['available_number'], item['pub_date'] = requirements
            item['edu'] = 'Unknown'
        text = ''.join([each.strip()for each in sel.css('div .job_msg::text').extract()])
        attribute = ['duty', 'demand', 'welfare', 'work_time', 'address']
        item['duty'] = item['demand'] = item['welfare'] = item['work_time'] = item['address'] = ''
        flags = []
        # Point out positions of above key words
        for words in self.KEY_WORDS:
            for word in words:
                flag = text.find(word)
                if flag != -1:
                    flags.append(flag)
                    break
        flags.sort()
        # Then, process these positions one by one
        for flag in range(len(flags)):
            quit_not = False
            # cut the string into various sections
            if flag == len(flags) - 1:
                message = text[flags[flag]:]
            else:
                message = text[flags[flag]:flags[flag + 1]]
            # For each section, we will find the key words of it.
            for n, words in enumerate(self.KEY_WORDS):
                for word in words:
                    if word in message:
                        key = attribute[n]
                        # cut off some unnecessary words
                        word_length = len(word)
                        item[key] = message[word_length:]
                        quit_not = True
                        break
                if quit_not:
                    break
        address = ''.join(sel.css('div.bmsg>p.fp::text').extract()).strip()
        if location not in self.city_list:
            for city in self.city_list:
                if city in address:
                    item['location'] = city
                    break
            else:
                logging.log(logging.WARNING, 'The city -- %s is not in the list,url:%s' % (location, response.url))
        item['address'] = address
        item['job_category'] = 'Unknown'
        yield item
