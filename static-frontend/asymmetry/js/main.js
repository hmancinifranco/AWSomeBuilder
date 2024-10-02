;(function () {
	
	'use strict';

	var isMobile = {
		Android: function() {
			return navigator.userAgent.match(/Android/i);
		},
			BlackBerry: function() {
			return navigator.userAgent.match(/BlackBerry/i);
		},
			iOS: function() {
			return navigator.userAgent.match(/iPhone|iPad|iPod/i);
		},
			Opera: function() {
			return navigator.userAgent.match(/Opera Mini/i);
		},
			Windows: function() {
			return navigator.userAgent.match(/IEMobile/i);
		},
			any: function() {
			return (isMobile.Android() || isMobile.BlackBerry() || isMobile.iOS() || isMobile.Opera() || isMobile.Windows());
		}
	};

	var fullHeight = function() {

		if ( !isMobile.any() ) {
			$('.js-fullheight').css('height', $(window).height());
			$(window).resize(function(){
				$('.js-fullheight').css('height', $(window).height());
			});
		}

	};

	var mobileMenuOutsideClick = function() {

		$(document).click(function (e) {
	    var container = $("#gtco-offcanvas, .js-gtco-nav-toggle");
	    if (!container.is(e.target) && container.has(e.target).length === 0) {
	    	if ( $('body').hasClass('offcanvas') ) {
    			$('body').removeClass('offcanvas');
    			$('.js-gtco-nav-toggle').removeClass('active');
	    	}
	    }
		});

	};

	

	var header = function() {
		$('.header-fixed').css('padding-top', $('.gtco-nav').height());
	};

	var navigation = function() {

		$('body').on('click', '#gtco-offcanvas ul a:not([class="external"]), .main-nav a:not([class="external"])', function(event){
			var section = $(this).data('nav-section');
				if ( $('[data-section="' + section + '"]').length ) {
			    	$('html, body').animate({
			        	scrollTop: $('[data-section="' + section + '"]').offset().top - 55
			    	}, 500, 'easeInOutExpo');
			   }

			   if ($('body').hasClass('offcanvas')) {
			   	$('body').removeClass('offcanvas');
			   	$('.js-gtco-nav-toggle').removeClass('active');
			   }
		   event.preventDefault();
		   return false;
		});

	};


	var offcanvasMenu = function() {

		$('body').prepend('<div id="gtco-offcanvas" />');
		$('body').prepend('<a href="#" class="js-gtco-nav-toggle gtco-nav-toggle"><i></i></a>');
		var clone1 = $('.menu-1 > ul').clone();
		$('#gtco-offcanvas').append(clone1);
		var clone2 = $('.menu-2 > ul').clone();
		$('#gtco-offcanvas').append(clone2);

		$('#gtco-offcanvas .has-dropdown').addClass('offcanvas-has-dropdown');
		$('#gtco-offcanvas')
			.find('li')
			.removeClass('has-dropdown');

		// Hover dropdown menu on mobile
		$('.offcanvas-has-dropdown').mouseenter(function(){
			var $this = $(this);

			$this
				.addClass('active')
				.find('ul')
				.slideDown(500, 'easeOutExpo');				
		}).mouseleave(function(){

			var $this = $(this);
			$this
				.removeClass('active')
				.find('ul')
				.slideUp(500, 'easeOutExpo');				
		});


		$(window).resize(function(){

			if ( $('body').hasClass('offcanvas') ) {

    			$('body').removeClass('offcanvas');
    			$('.js-gtco-nav-toggle').removeClass('active');
				
	    	}
		});
	};




	// Reflect scrolling in navigation
	var navActive = function(section) {

		var $el = $('.main-nav > ul');
		$el.find('li').removeClass('active');
		$el.each(function(){
			$(this).find('a[data-nav-section="'+section+'"]').closest('li').addClass('active');
		});

	};

	var navigationSection = function() {

		var $section = $('div[data-section]');
		
		$section.waypoint(function(direction) {
		  	
		  	if (direction === 'down') {
		    	navActive($(this.element).data('section'));
		  	}
		}, {
	  		offset: '150px'
		});

		$section.waypoint(function(direction) {
		  	if (direction === 'up') {
		    	navActive($(this.element).data('section'));
		  	}
		}, {
		  	offset: function() { return -$(this.element).height() + 155; }
		});

	};

	var burgerMenu = function() {

		$('body').on('click', '.js-gtco-nav-toggle', function(event){
			var $this = $(this);


			if ( $('body').hasClass('offcanvas') ) {
				$('body').removeClass('offcanvas');
			} else {
				$('body').addClass('offcanvas');
			}
			$this.toggleClass('active');
			event.preventDefault();

		});
	};



	var contentWayPoint = function() {
		var i = 0;
		$('.animate-box').waypoint( function( direction ) {

			if( direction === 'down' && !$(this.element).hasClass('animated-fast') ) {
				
				i++;

				$(this.element).addClass('item-animate');
				setTimeout(function(){

					$('body .animate-box.item-animate').each(function(k){
						var el = $(this);
						setTimeout( function () {
							var effect = el.data('animate-effect');
							if ( effect === 'fadeIn') {
								el.addClass('fadeIn animated-fast');
							} else if ( effect === 'fadeInLeft') {
								el.addClass('fadeInLeft animated-fast');
							} else if ( effect === 'fadeInRight') {
								el.addClass('fadeInRight animated-fast');
							} else {
								el.addClass('fadeInUp animated-fast');
							}

							el.removeClass('item-animate');
						},  k * 200, 'easeInOutExpo' );
					});
					
				}, 100);
				
			}

		} , { offset: '85%' } );
	};


	var dropdown = function() {

		$('.has-dropdown').mouseenter(function(){

			var $this = $(this);
			$this
				.find('.dropdown')
				.css('display', 'block')
				.addClass('animated-fast fadeInUpMenu');

		}).mouseleave(function(){
			var $this = $(this);

			$this
				.find('.dropdown')
				.css('display', 'none')
				.removeClass('animated-fast fadeInUpMenu');
		});

	};


	var owlCarousel = function(){
		
		var owl = $('.owl-carousel-carousel');
		owl.owlCarousel({
			items: 3,
			loop: true,
			margin: 20,
			nav: true,
			dots: true,
			smartSpeed: 800,
			navText: [
		      "<i class='ti-arrow-left owl-direction'></i>",
		      "<i class='ti-arrow-right owl-direction'></i>"
	     	],
	     	responsive:{
	        0:{
	            items:1
	        },
	        600:{
	            items:2
	        },
	        1000:{
	            items:3
	        }
	    	}
		});


		var owl = $('.owl-carousel-fullwidth');
		owl.owlCarousel({
			items: 1,
			loop: true,
			margin: 20,
			nav: true,
			dots: true,
			smartSpeed: 800,
			autoHeight: true,
			navText: [
		      "<i class='ti-arrow-left owl-direction'></i>",
		      "<i class='ti-arrow-right owl-direction'></i>"
	     	]
		});

	};


	var goToTop = function() {

		$('.js-gotop').on('click', function(event){
			
			event.preventDefault();

			$('html, body').animate({
				scrollTop: $('html').offset().top
			}, 500, 'easeInOutExpo');
			
			return false;
		});

		$(window).scroll(function(){

			var $win = $(window);
			if ($win.scrollTop() > 200) {
				$('.js-top').addClass('active');
			} else {
				$('.js-top').removeClass('active');
			}

		});
	
	};


	// Loading page
	var loaderPage = function() {
		$(".gtco-loader").fadeOut("slow");
	};

	var counter = function() {
		$('.js-counter').countTo({
			 formatter: function (value, options) {
	      return value.toFixed(options.decimals);
	    },
		});
	};

	var counterWayPoint = function() {
		if ($('#gtco-counter').length > 0 ) {
			$('#gtco-counter').waypoint( function( direction ) {
										
				if( direction === 'down' && !$(this.element).hasClass('animated') ) {
					setTimeout( counter , 400);					
					$(this.element).addClass('animated');
				}
			} , { offset: '90%' } );
		}
	};

	var accordion = function() {
		$('.gtco-accordion-heading').on('click', function(event){

			var $this = $(this);

			$this.closest('.gtco-accordion').find('.gtco-accordion-content').slideToggle(400, 'easeInOutExpo');
			if ($this.closest('.gtco-accordion').hasClass('active')) {
				$this.closest('.gtco-accordion').removeClass('active');
			} else {
				$this.closest('.gtco-accordion').addClass('active');
			}
			event.preventDefault();
		});
	};

	var sliderMain = function() {
		
	  	$('#gtco-hero .flexslider').flexslider({
			animation: "fade",
			slideshowSpeed: 5000,
			directionNav: true,
			start: function(){
				setTimeout(function(){
					$('.slider-text').removeClass('animated fadeInUp');
					$('.flex-active-slide').find('.slider-text').addClass('animated fadeInUp');
				}, 500);
			},
			before: function(){
				setTimeout(function(){
					$('.slider-text').removeClass('animated fadeInUp');
					$('.flex-active-slide').find('.slider-text').addClass('animated fadeInUp');
				}, 500);
			}

	  	});

	  	$('#gtco-hero .flexslider .slides > li').css('height', $(window).height());	
	  	$(window).resize(function(){
	  		$('#gtco-hero .flexslider .slides > li').css('height', $(window).height());	
	  	});

	};


	//new addup to navegating from carrousel
	document.addEventListener('click', function(event) {
		let target = event.target;
	
		// Check if the clicked element or its parent has a data-nav-section attribute
		while (target && !target.matches('body')) {
			if (target.hasAttribute('data-nav-section')) {
				event.preventDefault();
				let section = target.getAttribute('data-nav-section');
				navigateToSection(section);
				return;
			}
			target = target.parentElement;
		}
	});
	
	function navigateToSection(section) {
		const sectionElement = document.querySelector(`[data-section="${section}"]`);
		if (sectionElement) {
			sectionElement.scrollIntoView({ behavior: 'smooth' });
		}
	}
	

		// function to restrict formats in upload and classify form text boxes

		document.addEventListener('DOMContentLoaded', function() {
			const yearMonthInput = document.getElementById('year-month');
		
			yearMonthInput.addEventListener('input', function(e) {
				let input = e.target.value;
				
				// Remove any non-digit characters
				input = input.replace(/\D/g, '');
				
				// Insert hyphen after the first 4 digits
				if (input.length > 4) {
					input = input.slice(0, 4) + '-' + input.slice(4);
				}
				
				// Truncate to max length
				input = input.slice(0, 7);
				
				e.target.value = input;
			});
		
			yearMonthInput.addEventListener('blur', function(e) {
				let input = e.target.value;
				
				// Ensure the input is in the correct format when leaving the field
				if (input.length === 7 && input.match(/^\d{4}-\d{2}$/)) {
					// Valid format, do nothing
				} else {
					// Invalid format, clear the input
					e.target.value = '';
					alert('Please enter a valid year-month in YYYY-MM format.');
				}
			});
		});

		document.addEventListener('DOMContentLoaded', function() {
			const customerInput = document.getElementById('customer');
		
			customerInput.addEventListener('input', function(e) {
				let input = e.target.value;
		
				// Remove any non-alphanumeric and non-hyphen characters
				input = input.replace(/[^a-z0-9-]/gi, '');
		
				// Convert to lowercase
				input = input.toLowerCase();
		
				// Truncate to max length of 50 characters
				input = input.slice(0, 50);
		
				e.target.value = input;
			});
		
			customerInput.addEventListener('blur', function(e) {
				let input = e.target.value;
		
				// Ensure the input contains only alphanumeric and hyphen characters
				if (input.match(/^[a-z0-9-]+$/)) {
					// Valid format, do nothing
				} else {
					// Invalid format, clear the input
					e.target.value = '';
					alert('Please enter a valid customer name using only letters, numbers, and hyphens.');
				}
			});
		});

		//Function to query dynamodb table using API Gateway:
// Función para enviar una solicitud a la API Gateway
async function sendRequest(endpoint, params) {
	const apiUrl = `https://${apiGatewayId}.execute-api.${region}.amazonaws.com/${stage}/${endpoint}`;
	const response = await fetch(apiUrl, {
	  method: 'POST',
	  headers: {
		'Content-Type': 'application/json'
	  },
	  body: JSON.stringify(params)
	});
	const data = await response.json();
	return data;
  }
  
  // Función para manejar el envío del formulario
  document.getElementById('queryForm').addEventListener('submit', async (event) => {
	event.preventDefault();
  
	const customer = document.getElementById('customer-query').value;
	const yearMonth = document.getElementById('year-month-query').value;
  
	let endpoint, params;
  
	if (customer && yearMonth) {
	  endpoint = 'query-customer-year-month';
	  params = { customer, yearMonth };
	} else if (customer) {
	  endpoint = 'query-customer';
	  params = { customer };
	} else {
	  // No se permiten otras combinaciones
	  alert('Debe ingresar al menos el nombre del cliente');
	  return;
	}
  
	const results = await sendRequest(endpoint, params);
	displayResults(results);
  });
  
  // Función para mostrar los resultados en la tabla HTML
  function displayResults(results) {
	const tableBody = document.getElementById('resultsTable').getElementsByTagName('tbody')[0];
	tableBody.innerHTML = '';
  
	results.forEach(item => {
	  const row = tableBody.insertRow();
	  const customerCell = row.insertCell(0);
	  const yearMonthCell = row.insertCell(1);
  
	  customerCell.textContent = item.customer;
	  yearMonthCell.textContent = item.year_month || ''; // Muestra un campo vacío si year_month no existe
	});
  }
		

	
	$(function(){
		fullHeight();
		mobileMenuOutsideClick();
		header();
		navigation();
		offcanvasMenu();
		burgerMenu();
		navigationSection();
		contentWayPoint();
		dropdown();
		owlCarousel();
		goToTop();
		loaderPage();
		counterWayPoint();
		accordion();
		sliderMain();
	});


}());