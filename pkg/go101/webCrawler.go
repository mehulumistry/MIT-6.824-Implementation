package main

import (
	"context"
	"fmt"
	"time"
)

type UrlMetadata struct {
	url   string
	depth int
}

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

func ctxCrawlerCoordinator(ctx context.Context, url string, fetcher Fetcher) {
	set := map[string]bool{}
	coordinatorChannel := make(chan []string)

	// Initial send with context
	go func() {
		coordinatorChannel <- []string{url}
	}()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Coordinator received cancellation")
			return // Terminate coordinator
		case urls := <-coordinatorChannel:
			for _, url := range urls {
				if !set[url] {
					set[url] = true

					// Pass context to workers
					go ctxWorkers(ctx, coordinatorChannel, url, fetcher)
				}
			}
		}
	}
}

func ctxWorkers(ctx context.Context, ch chan []string, url string, fetcher Fetcher) {
	body, urls, err := fetcher.Fetch(url)

	select {
	case <-ctx.Done():
		fmt.Println("Worker for", url, "received cancellation")
		return // Exit worker
	default:
		if err != nil {
			fmt.Println(err)
			ch <- []string{}
			return // Avoid passing nil urls along
		}
		fmt.Printf("found: %s %q\n", url, body)
		ch <- urls
	}
}

func crawlerCoordinator(url string, fetcher Fetcher) {

	// Decides the depth
	// distribute the work to the workers to fetch more urls
	set := map[string]bool{}

	coordinatorChannel := make(chan []string)
	go func() {
		coordinatorChannel <- []string{url}
	}()

	//println("Adding urls: ", url)

	//n := 1

	for {
		urls, _ := <-coordinatorChannel

		for _, url := range urls {
			if !set[url] {
				//n += 1
				set[url] = true
				go workers(coordinatorChannel, url, fetcher)
			}
		}

		//n -= 1
		//if n == 0 {
		//	break
		//}
	}

}

func workers(ch chan []string, url string, fetcher Fetcher) {
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		ch <- []string{}
		// NOTE: I just changed this and worked, revert back your changes
		return
	}
	fmt.Printf("found: %s %q\n", url, body)
	ch <- urls
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher, set map[string]bool) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	if depth <= 0 {
		return
	}

	// Check if already fetched.

	if !set[url] {
		body, urls, err := fetcher.Fetch(url)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("found: %s %q\n", url, body)
		set[url] = true
		for _, u := range urls {
			Crawl(u, depth-1, fetcher, set)
		}
	}
	return
}

func main() {
	//set := map[string]bool{}
	//crawlerCoordinator("https://golang.org/", fetcher)
	//Crawl("https://golang.org/", 4, fetcher, set)

	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second) // Timeout example
	defer cancelFunc()                                                           // Ensure cancellation
	ctxCrawlerCoordinator(ctx, "https://golang.org/", fetcher)
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
