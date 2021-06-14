import concurrent.futures
import threading
import time
import twitter_worker
import doi_resolver

thread_local = threading.local()



if __name__ == "__main__":
    start = time.time()
    # urls = [
    #     "https://doi.org/10.1242/jeb.224485",
    #     "https://doi.org/10.1242/jeb.224485",
    #     "https://doi.org/10.1242/jeb.224485",
    #     "http://dx.doi.org/10.1016/j.redox.2021.101988",
    #     "https://www.emerald.com/insight/content/doi/10.1108/INTR-01-2020-0038/full/html",
    #     "https://www.sciencedirect.com/science/article/pii/S1934590921001594",
    #     "https://www.degruyter.com/document/doi/10.7208/9780226733050/html",
    #     "https://link.springer.com/article/10.1007/s00467-021-05115-7",
    #     "https://onlinelibrary.wiley.com/doi/10.1111/andr.13003",
    #     "https://www.nature.com/articles/s41398-021-01387-7",
    #     "https://science.sciencemag.org/content/372/6543/694.1.full",
    #     "https://journals.sagepub.com/doi/10.1177/00469580211005191",
    #     "https://journals.plos.org/ploscompbiol/article?id=10.1371/journal.pcbi.1008922",
    #     "https://www.frontiersin.org/articles/10.3389/fnume.2021.671914/full",
    #     "https://www.tandfonline.com/doi/full/10.1080/09638237.2021.1898552",
    #     "https://www.mdpi.com/2072-4292/13/10/1955",
    #     "https://iopscience.iop.org/article/10.1088/1361-6528/abfee9/meta",
    #     "https://www.cochranelibrary.com/cdsr/doi/10.1002/14651858.CD013263.pub2/full",
    #     "https://www.nejm.org/doi/full/10.1056/NEJMcibr2034927",
    #     "https://www.thelancet.com/journals/eclinm/article/PIIS2589-5370(20)30464-8/fulltext",
    #     "https://www.bmj.com/content/373/bmj.n922",
    #     "https://www.pnas.org/content/117/48/30071",
    #     "https://jamanetwork.com/journals/jamaneurology/article-abstract/2780249",
    #     "https://www.acpjournals.org/doi/10.7326/G20-0087",
    #     "https://n.neurology.org/content/96/19/e2414.abstract",
    #     "https://doi.apa.org/record/1988-31508-001",
    #     "https://ieeexplore.ieee.org/document/9430520",
    #     "https://dl.acm.org/doi/abs/10.1145/3411764.3445371",
    #     "https://jmir.org/2021/5/e26618",
    #     "https://journals.aps.org/pra/abstract/10.1103/PhysRevA.103.053314",
    #     "https://www.biorxiv.org/content/10.1101/2021.05.14.444134v1",
    #     "https://arxiv.org/abs/2103.11251",
    #     "https://academic.oup.com/glycob/advance-article-abstract/doi/10.1093/glycob/cwab035/6274761#.YKKxIEAvSvs.twitter",
    #     "https://www.jmcc-online.com/article/S0022-2828(21)00101-2/fulltext"
    # ]
    # with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    #         executor.map(doi_resolver.link_url, urls)
    # print(time.time() - start)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.submit(twitter_worker.startWorker())
            executor.submit(twitter_worker.startWorker())
            executor.submit(twitter_worker.startWorker())
            executor.submit(twitter_worker.startWorker())
            executor.submit(twitter_worker.startWorker())
            executor.submit(twitter_worker.startWorker())
            executor.submit(twitter_worker.startWorker())
            executor.submit(twitter_worker.startWorker())
            executor.submit(twitter_worker.startWorker())
            executor.submit(twitter_worker.startWorker())
    print(time.time() - start)