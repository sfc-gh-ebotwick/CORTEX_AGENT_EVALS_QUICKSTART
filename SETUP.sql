-- ====================================================================
-- MARKETING CAMPAIGNS ANALYTICS SYSTEM - COMPLETE SETUP SCRIPT
-- ====================================================================
-- This script creates a complete marketing campaigns analytics system with:
-- - Database and tables with sample data
-- - Semantic view for Cortex Analyst
-- - Cortex Search service for content discovery
-- - Stored procedure for report generation
-- - Cortex Agent integrating all tools
--
-- Prerequisites:
-- - ACCOUNTADMIN role (or role with CREATE DATABASE privileges)
-- - Access to COMPUTE_WH warehouse (or modify warehouse name below)
-- - SNOWFLAKE.CORTEX_USER database role granted
--
-- Estimated runtime: 5-10 minutes
-- ====================================================================

USE ROLE ACCOUNTADMIN;

-- ====================================================================
-- CONFIGURATION VARIABLES
-- ====================================================================
-- Set your database name here - modify this value to use a different database name
-- ====================================================================
-- SECTION 1: DATABASE AND TABLE CREATION
-- ====================================================================

-- Create database using variable
CREATE DATABASE IF NOT EXISTS MARKETING_CAMPAIGNS_DB;
USE DATABASE MARKETING_CAMPAIGNS_DB;
USE SCHEMA PUBLIC;

-- Create CAMPAIGNS table
CREATE OR REPLACE TABLE CAMPAIGNS (
    campaign_id INT AUTOINCREMENT PRIMARY KEY,
    campaign_name VARCHAR(200) NOT NULL,
    campaign_type VARCHAR(50),
    start_date DATE,
    end_date DATE,
    budget_allocated DECIMAL(12,2),
    target_audience VARCHAR(200),
    channel VARCHAR(50),
    status VARCHAR(50),
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create CAMPAIGN_PERFORMANCE table
CREATE OR REPLACE TABLE CAMPAIGN_PERFORMANCE (
    performance_id INT AUTOINCREMENT PRIMARY KEY,
    campaign_id INT,
    date DATE,
    impressions INT,
    clicks INT,
    conversions INT,
    cost_per_click DECIMAL(10,4),
    cost_per_acquisition DECIMAL(10,2),
    revenue_generated DECIMAL(12,2),
    roi_percentage DECIMAL(8,2),
    engagement_rate DECIMAL(8,4),
    FOREIGN KEY (campaign_id) REFERENCES CAMPAIGNS(campaign_id)
);

-- Create CAMPAIGN_CONTENT table
CREATE OR REPLACE TABLE CAMPAIGN_CONTENT (
    content_id INT AUTOINCREMENT PRIMARY KEY,
    campaign_id INT,
    content_type VARCHAR(100),
    campaign_description TEXT,
    creative_assets VARIANT,
    marketing_copy TEXT,
    a_b_test_notes TEXT,
    sentiment_analysis VARIANT,
    FOREIGN KEY (campaign_id) REFERENCES CAMPAIGNS(campaign_id)
);

-- Create CAMPAIGN_FEEDBACK table
CREATE OR REPLACE TABLE CAMPAIGN_FEEDBACK (
    feedback_id INT AUTOINCREMENT PRIMARY KEY,
    campaign_id INT,
    feedback_date DATE,
    customer_segment VARCHAR(100),
    satisfaction_score DECIMAL(3,2),
    detailed_comments TEXT,
    survey_responses VARIANT,
    recommended_improvements TEXT,
    FOREIGN KEY (campaign_id) REFERENCES CAMPAIGNS(campaign_id)
);

-- ====================================================================
-- SECTION 2: INSERT SAMPLE CAMPAIGN DATA
-- ====================================================================

INSERT INTO CAMPAIGNS (campaign_name, campaign_type, start_date, end_date, budget_allocated, target_audience, channel, status, created_by) VALUES
('Spring Fashion Launch 2024', 'Product Launch', '2024-03-01', '2024-04-30', 75000.00, 'Women 25-45, Fashion Enthusiasts', 'social', 'completed', 'sarah.marketing'),
('Q1 Email Nurture Series', 'Lead Nurturing', '2024-01-15', '2024-03-31', 25000.00, 'B2B Decision Makers', 'email', 'completed', 'john.campaigns'),
('Summer Sale Extravaganza', 'Promotional', '2024-06-01', '2024-07-15', 120000.00, 'All Customers, Deal Seekers', 'display', 'completed', 'sarah.marketing'),
('Brand Awareness - Tech Sector', 'Brand Building', '2024-02-01', '2024-05-31', 200000.00, 'Tech Professionals 30-55', 'search', 'completed', 'michael.brand'),
('Black Friday Mega Sale', 'Seasonal', '2024-11-15', '2024-11-30', 300000.00, 'All Segments', 'social', 'active', 'sarah.marketing'),
('New Product Teaser Campaign', 'Product Launch', '2024-09-01', '2024-10-15', 45000.00, 'Early Adopters, Tech Enthusiasts', 'social', 'completed', 'emily.digital'),
('Customer Retention Q2', 'Retention', '2024-04-01', '2024-06-30', 60000.00, 'Existing Customers', 'email', 'completed', 'john.campaigns'),
('Holiday Gift Guide 2024', 'Seasonal', '2024-11-01', '2024-12-24', 150000.00, 'Gift Shoppers, All Ages', 'display', 'active', 'sarah.marketing'),
('LinkedIn B2B Lead Gen', 'Lead Generation', '2024-05-01', '2024-08-31', 85000.00, 'Enterprise Decision Makers', 'social', 'completed', 'michael.brand'),
('Back to School Campaign', 'Seasonal', '2024-08-01', '2024-09-15', 95000.00, 'Parents, Students 18-24', 'search', 'completed', 'emily.digital'),
('Influencer Partnership Series', 'Brand Building', '2024-03-15', '2024-06-30', 110000.00, 'Millennials, Gen Z', 'social', 'completed', 'sarah.marketing'),
('Re-engagement Email Blast', 'Retention', '2024-07-01', '2024-07-31', 15000.00, 'Inactive Users 90+ days', 'email', 'completed', 'john.campaigns'),
('Premium Membership Launch', 'Product Launch', '2024-10-01', '2024-11-30', 180000.00, 'High-Value Customers', 'email', 'active', 'michael.brand'),
('Valentine Day Special', 'Seasonal', '2024-02-01', '2024-02-14', 55000.00, 'Couples 25-50', 'display', 'completed', 'emily.digital'),
('Mobile App Download Campaign', 'App Promotion', '2024-06-15', '2024-08-31', 70000.00, 'Mobile-First Users', 'search', 'completed', 'sarah.marketing'),
('Sustainability Initiative Awareness', 'Brand Building', '2024-04-15', '2024-07-31', 90000.00, 'Eco-Conscious Consumers', 'social', 'completed', 'michael.brand'),
('Flash Sale - 48 Hours', 'Promotional', '2024-05-20', '2024-05-22', 30000.00, 'Price-Sensitive Shoppers', 'email', 'completed', 'emily.digital'),
('Year-End Clearance', 'Promotional', '2024-12-01', '2024-12-31', 135000.00, 'Bargain Hunters', 'display', 'active', 'sarah.marketing'),
('Webinar Series Promotion', 'Lead Generation', '2024-03-01', '2024-05-31', 40000.00, 'B2B Professionals', 'email', 'completed', 'john.campaigns'),
('Customer Testimonials Campaign', 'Social Proof', '2024-08-15', '2024-10-31', 50000.00, 'Prospective Customers', 'social', 'completed', 'michael.brand'),
('Cyber Monday Deals', 'Seasonal', '2024-11-28', '2024-12-02', 250000.00, 'Online Shoppers', 'search', 'active', 'emily.digital'),
('Referral Program Launch', 'Customer Acquisition', '2024-07-15', '2024-09-30', 65000.00, 'Existing Happy Customers', 'email', 'completed', 'john.campaigns'),
('New Market Expansion - West Coast', 'Geographic Expansion', '2024-09-15', '2024-12-31', 175000.00, 'West Coast Residents', 'display', 'active', 'michael.brand'),
('Product Demo Video Series', 'Educational', '2024-04-01', '2024-06-30', 55000.00, 'Consideration Stage Leads', 'social', 'completed', 'sarah.marketing'),
('Anniversary Sale - 5 Years', 'Promotional', '2024-10-15', '2024-10-31', 100000.00, 'Loyal Customers', 'email', 'completed', 'emily.digital');

-- ====================================================================
-- SECTION 3: INSERT PERFORMANCE DATA
-- ====================================================================

INSERT INTO CAMPAIGN_PERFORMANCE (campaign_id, date, impressions, clicks, conversions, cost_per_click, cost_per_acquisition, revenue_generated, roi_percentage, engagement_rate) 
SELECT 
    c.campaign_id,
    DATEADD(day, seq.seq, c.start_date) as date,
    UNIFORM(50000, 200000, RANDOM()) as impressions,
    CASE 
        WHEN c.channel = 'email' THEN UNIFORM(2000, 8000, RANDOM())
        WHEN c.channel = 'social' THEN UNIFORM(1000, 5000, RANDOM())
        WHEN c.channel = 'search' THEN UNIFORM(3000, 10000, RANDOM())
        ELSE UNIFORM(1500, 6000, RANDOM())
    END as clicks,
    CASE 
        WHEN c.channel = 'email' THEN UNIFORM(50, 400, RANDOM())
        WHEN c.channel = 'social' THEN UNIFORM(30, 250, RANDOM())
        WHEN c.channel = 'search' THEN UNIFORM(100, 500, RANDOM())
        ELSE UNIFORM(40, 300, RANDOM())
    END as conversions,
    ROUND(UNIFORM(0.50, 4.50, RANDOM()), 2) as cost_per_click,
    ROUND(UNIFORM(15.00, 95.00, RANDOM()), 2) as cost_per_acquisition,
    ROUND(UNIFORM(5000, 45000, RANDOM()), 2) as revenue_generated,
    ROUND(UNIFORM(-25.00, 350.00, RANDOM()), 2) as roi_percentage,
    ROUND(UNIFORM(0.0150, 0.0850, RANDOM()), 4) as engagement_rate
FROM CAMPAIGNS c
CROSS JOIN (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 120))
) seq
WHERE DATEADD(day, seq.seq, c.start_date) <= c.end_date
    AND DATEADD(day, seq.seq, c.start_date) <= CURRENT_DATE();

-- ====================================================================
-- SECTION 4: INSERT CAMPAIGN CONTENT
-- ====================================================================

INSERT INTO CAMPAIGN_CONTENT (campaign_id, content_type, campaign_description, marketing_copy, a_b_test_notes) VALUES
(1, 'Social Media + Display', 'Spring Fashion Launch targeting women 25-45 with new seasonal collection. Focus on sustainable materials, vibrant colors, and versatile pieces. Strategy includes Instagram influencer partnerships, Pinterest boards, and Facebook carousel ads.', 'Refresh your spring wardrobe with sustainable style! Our new collection combines eco-friendly materials with trend-setting designs. Shop now and get 20% off your first spring purchase.', 'A/B Test: Version A (20% discount) outperformed Version B (free shipping) by 35% in conversion rate. Imagery with models in outdoor settings performed 28% better than studio shots.'),
(2, 'Email Marketing', 'Q1 B2B email nurture series for decision makers. Six-email journey covering industry trends, thought leadership, case studies, and product education. Personalized content based on industry segment and company size.', 'Subject Line: 3 Trends Reshaping Your Industry in 2024. Learn how innovative companies are cutting costs by 40% while improving efficiency.', 'Best performing: Email 3 (case study) with 45% open rate, 12% CTR. Personalized subject lines increased opens by 22%. Tuesday 10AM sends performed best.'),
(3, 'Display + Retargeting', 'Summer Sale campaign with aggressive discounts up to 60% off. Multi-channel display advertising with dynamic retargeting. Creative emphasizes urgency, scarcity, and deal value.', 'SUMMER SALE IS HERE! Up to 60% OFF Everything! Limited time only. Shop top brands, hottest styles, unbeatable prices. Free shipping on orders over $50. Sale ends July 15th!', 'Urgency messaging increased CTR by 48%. Countdown timers improved conversion by 31%. Retargeting ads showing previously viewed products had 4.2x higher conversion.'),
(4, 'Search + Content Marketing', 'Brand awareness campaign targeting tech sector professionals. Strategy includes thought leadership content, industry reports, expert interviews, and educational resources.', 'Navigate the evolving tech landscape with confidence. Download our comprehensive 2024 Tech Trends Report featuring insights from 500+ industry leaders.', 'Long-form content generated 3x more leads. Gated content strategy effective. Search ads for tech trends had lowest CPA at $23.'),
(5, 'Multi-Channel', 'Black Friday mega sale campaign across all channels. Massive promotional push with doorbusters, flash sales, and exclusive early access for email subscribers.', 'BLACK FRIDAY IS HERE! Doorbuster deals starting at midnight. Early VIP access for email subscribers. Shop the biggest deals of the year!', 'Mobile traffic exceeded 75%. Push notifications had 89% open rate. Early access emails generated 45% of day-one revenue.'),
(6, 'Social Media', 'New product teaser campaign building anticipation for upcoming tech product launch. Cryptic messaging and gradual reveal strategy.', 'Something innovative is coming. Can you guess what it is? Follow for daily clues. Early bird pre-orders opening soon.', 'Teaser strategy generated 2.3M impressions. Waitlist sign-ups exceeded target by 340%.'),
(7, 'Email Marketing', 'Customer retention campaign targeting existing customers with personalized offers based on purchase history. Loyalty rewards and VIP experiences.', 'You are valued! Enjoy exclusive rewards: 25% off, early access to new collections, free shipping for life, and VIP customer service.', 'Tier-based personalization increased response by 67%. VIP tier had 89% retention rate.'),
(8, 'Display + Social', 'Holiday Gift Guide campaign featuring curated product recommendations. Interactive gift finder tool and expert recommendations.', 'Find the perfect gift for everyone! Our Holiday Gift Guide features expertly curated selections. Free gift wrapping. Guaranteed delivery by Dec 24th.', 'Interactive gift finder had 56% completion rate. Cross-sell suggestions increased AOV by $43.'),
(9, 'LinkedIn B2B', 'LinkedIn lead generation campaign targeting enterprise decision makers. Sponsored content, InMail campaigns, and thought leadership articles.', 'Is your enterprise ready for digital transformation? Learn how Fortune 500 companies are reducing costs by 40%. Download our case study.', 'LinkedIn InMail had 42% open rate. Case study downloads qualified leads effectively.'),
(10, 'Search + Display', 'Back to School campaign targeting parents and students. Product bundles, student discounts, and dorm essentials.', 'Get ready for back to school! Student discounts on everything you need. 15% off with student ID. Free shipping over $50.', 'Bundle offers increased AOV by $67. August timing critical for this campaign.'),
(11, 'Influencer Marketing', 'Influencer partnership series with micro and macro influencers across fashion, lifestyle, and beauty verticals.', 'Real people, real style. Check out how our brand ambassadors are styling our latest collection. Join the community!', 'Micro-influencers had best engagement rates at 8.7%. Video content 4x more effective than static posts.'),
(12, 'Email Re-engagement', 'Re-engagement campaign targeting users inactive for 90+ days. Win-back strategy with compelling offers.', 'We miss you! Check out everything new. Special 30% off just for you. Come back and see what you have been missing!', 'Three-email series optimal. Final email had 31% open rate. 15% of recipients reactivated.'),
(13, 'Multi-Channel Launch', 'Premium membership program launch offering exclusive benefits. Tiered membership structure focused on high-value customers.', 'Introducing Premium Membership. Unlimited free shipping, early access to sales, exclusive pricing, priority support.', 'ROI calculator increased conversions 43%. Annual payment option had 68% take rate.'),
(14, 'Display Ads', 'Valentine Day Special campaign targeting couples with romantic gift ideas and special experiences.', 'Make this Valentine Day unforgettable! Romantic gifts, special experiences, guaranteed delivery by Feb 14th.', 'Emotional messaging resonated strongly. Gift bundle suggestions increased AOV by 52%.'),
(15, 'Search + Social', 'Mobile App Download Campaign promoting app-exclusive features and seamless mobile shopping experience.', 'Download our app for exclusive mobile-only deals! Push notifications for flash sales, easy checkout. Shop anywhere, anytime.', 'App install ads on social outperformed search by 3x. Push notification opt-in rate of 67%.'),
(16, 'Content Marketing', 'Sustainability Initiative Awareness campaign highlighting environmental commitments and eco-friendly products.', 'Join us in creating a sustainable future. Learn about our environmental commitments, carbon-neutral shipping, and eco-friendly products.', 'Authenticity crucial. Long-form content about sustainability efforts built trust. Eco-conscious audience willing to pay premium.'),
(17, 'Email Flash Sale', 'Flash Sale 48 Hours campaign with deep discounts and limited inventory to create urgency.', 'FLASH SALE! 48 HOURS ONLY! Up to 70% off select items. Limited quantities available. Shop now before they are gone!', 'Countdown timers increased CTR by 58%. Mobile traffic dominated at 82%.'),
(18, 'Display + Email', 'Year-End Clearance campaign to clear inventory with deep discounts across all categories.', 'Year-End Clearance! Final markdowns on everything. Up to 75% off. Last chance to save big!', 'Multi-wave email strategy kept campaign fresh. Deeper discounts over time created anticipation.'),
(19, 'Email Series', 'Webinar Series Promotion for B2B audience featuring industry experts and educational content.', 'Join our expert webinar series! Learn best practices, industry insights, and actionable strategies. Free registration.', 'Educational content positioned as valuable resource. Follow-up email with recording had 52% open rate.'),
(20, 'Social Proof', 'Customer Testimonials Campaign featuring real customer stories, reviews, and user-generated content.', 'Hear from our customers! Real stories, real results. Join thousands of satisfied customers.', 'Video testimonials outperformed text by 4x. Specific results increased credibility.'),
(21, 'Search + Social', 'Cyber Monday Deals campaign with aggressive online-only promotions across popular products.', 'CYBER MONDAY! Online-only deals. Biggest discounts of the year. Tech deals, fashion steals, home essentials!', 'Search volume surged 12x. Site performance critical. Mobile optimization crucial with 79% mobile traffic.'),
(22, 'Email + Referral', 'Referral Program Launch incentivizing existing customers to refer friends with rewards.', 'Share the love! Refer a friend and you both get $25 off. The more you refer, the more you save!', 'Double-sided incentive crucial. Email sharing most common. Simplified process increased completion by 64%.'),
(23, 'Display Ads', 'New Market Expansion West Coast campaign introducing brand to new geographic market.', 'Now serving the West Coast! Same great products, now available in your area. Local warehouse means faster shipping!', 'Localized creative essential. Regional influencer partnerships built credibility.'),
(24, 'Video Marketing', 'Product Demo Video Series showcasing product features and benefits through educational content.', 'See it in action! Our product demo series shows you how to get the most from your purchase. Step-by-step tutorials!', 'Video series kept viewers engaged. How-to content had highest completion rates.'),
(25, 'Email + Social', 'Anniversary Sale 5 Years celebration campaign thanking loyal customers with special offers.', 'Celebrating 5 years! Thank you for your support. Enjoy anniversary pricing, exclusive deals, and special surprises!', 'Nostalgic content resonated. Gratitude messaging strengthened emotional connection.');

-- ====================================================================
-- SECTION 5: INSERT FEEDBACK DATA
-- ====================================================================

INSERT INTO CAMPAIGN_FEEDBACK (campaign_id, feedback_date, customer_segment, satisfaction_score, detailed_comments, recommended_improvements) VALUES
(1, '2024-04-15', 'Fashion Enthusiasts', 4.5, 'Love the sustainable focus! Materials feel high quality and designs are on-trend. Pricing is reasonable for eco-friendly fashion. Wish there were more size options.', 'Add extended sizing. More color options in sustainable line.'),
(1, '2024-04-20', 'Eco-conscious Shoppers', 4.8, 'Finally a brand that cares about the environment! Transparency about sourcing appreciated. Fast shipping and beautiful packaging.', 'Share more about supply chain sustainability.'),
(2, '2024-03-25', 'B2B Decision Makers', 3.9, 'Content was informative but felt too generic at times. Case studies were helpful. Would like more industry-specific examples.', 'More personalization. Interactive content like assessments or calculators.'),
(3, '2024-07-10', 'Deal Seekers', 4.9, 'Incredible deals! Saved so much money. Website was easy to navigate. Checkout was smooth. Found everything on my wishlist.', 'Even longer sale period. More inventory on popular items.'),
(3, '2024-07-12', 'Price-Sensitive Shoppers', 4.7, 'Best sale of the year! Quality products at amazing prices. Free shipping made it even better. Will definitely shop future sales.', 'Email alerts when items are back in stock.'),
(4, '2024-05-28', 'Tech Professionals', 4.2, 'Thought-provoking content. Appreciated the data and research. Whitepapers were comprehensive. Helped with strategic planning.', 'More interactive webinars. Shorter executive summaries.'),
(5, '2024-11-25', 'Black Friday Shoppers', 4.6, 'Great deals but website was slow during peak hours. Found what I needed eventually. Push notifications were helpful.', 'Better site performance. Clearer deal expiration times.'),
(6, '2024-10-10', 'Tech Enthusiasts', 4.4, 'Teaser campaign was engaging and fun! Loved the mystery and daily reveals. Pre-order process was simple.', 'More interactive elements. Behind-the-scenes content.'),
(7, '2024-06-20', 'Loyal Customers', 4.9, 'Feel so appreciated! VIP perks are amazing. Free shipping saves me so much. Customer service is always helpful.', 'Birthday rewards. Exclusive product launches for VIP.'),
(8, '2024-12-15', 'Holiday Shoppers', 4.3, 'Gift guide made shopping so easy! Found perfect gifts for everyone. Gift wrapping service is convenient.', 'More price range options. Virtual shopping assistant.'),
(9, '2024-08-20', 'Enterprise Buyers', 4.1, 'Professional and informative content. Case studies demonstrated clear value. Free consultation was useful.', 'More technical deep-dives. ROI calculator tool.'),
(10, '2024-09-10', 'Parents', 4.5, 'One-stop shop for back to school! Bundle deals saved money. Student discount verification was easy.', 'Shopping lists by grade level. Bulk discounts.'),
(10, '2024-09-12', 'Students', 4.7, 'Student discount is awesome! Found everything I needed for dorm. Fast shipping before semester start.', 'Student rewards program. More tech accessories.'),
(11, '2024-06-25', 'Millennials', 4.6, 'Influencer content feels authentic. Love seeing real styling ideas. Discount codes are generous.', 'More diverse influencer partners. Styling videos.'),
(12, '2024-07-28', 'Inactive Users', 3.8, 'Glad I came back. Forgot how much I liked shopping here. 30% off was great incentive.', 'Understand why people leave. Easier re-engagement.'),
(13, '2024-11-20', 'Premium Members', 4.8, 'Membership pays for itself! Free shipping alone is worth it. Love early access to sales.', 'More member-exclusive products. Mobile app features.'),
(15, '2024-08-25', 'Mobile Users', 4.4, 'App is so convenient! Push notifications keep me updated. Mobile checkout is fast.', 'Apple Pay support. Wishlist sync across devices.'),
(16, '2024-07-22', 'Eco-conscious Consumers', 4.3, 'Appreciate sustainability commitment. Carbon-neutral shipping is great. Want to see more progress.', 'Regular sustainability reports. Product lifecycle info.'),
(17, '2024-05-21', 'Bargain Hunters', 4.9, 'Insane deals! Got everything on my list. 48-hour urgency made me act fast.', 'More frequent flash sales. Early access option.'),
(20, '2024-10-28', 'Prospective Customers', 4.2, 'Customer testimonials helped me decide to try. Real stories were convincing.', 'More video testimonials. Before/after examples.'),
(22, '2024-09-25', 'Referral Program Users', 4.5, 'Easy to refer friends! Love that we both get rewards. Already referred 5 people.', 'Higher rewards for multiple referrals. Social sharing.'),
(24, '2024-06-28', 'Product Researchers', 4.4, 'Demo videos answered all my questions. Saw product in action before buying.', 'Comparison videos. Customer Q&A sessions.'),
(25, '2024-10-29', 'Long-time Customers', 4.7, 'Been shopping here for years! Anniversary sale was wonderful. Feel valued as a customer.', 'Loyalty tiers based on years. Exclusive anniversary products.');

-- ====================================================================
-- SECTION 6: CREATE SEMANTIC VIEW
-- ====================================================================

CREATE OR REPLACE SEMANTIC VIEW MARKETING_PERFORMANCE_ANALYST
  TABLES (
    campaigns AS CAMPAIGNS PRIMARY KEY (campaign_id),
    performance AS CAMPAIGN_PERFORMANCE PRIMARY KEY (performance_id)
  )
  RELATIONSHIPS (
    performance(campaign_id) REFERENCES campaigns(campaign_id)
  )
  DIMENSIONS (
    PUBLIC campaigns.campaign_id AS campaign_id,
    PUBLIC campaigns.campaign_name AS campaign_name,
    PUBLIC campaigns.campaign_type AS campaign_type,
    PUBLIC campaigns.channel AS channel,
    PUBLIC campaigns.target_audience AS target_audience,
    PUBLIC campaigns.status AS status,
    PUBLIC campaigns.start_date AS start_date,
    PUBLIC campaigns.end_date AS end_date,
    PUBLIC campaigns.created_by AS created_by,
    PUBLIC performance.date AS date
  )
  METRICS (
    PUBLIC performance.total_revenue AS SUM(revenue_generated),
    PUBLIC performance.total_impressions AS SUM(impressions),
    PUBLIC performance.total_clicks AS SUM(clicks),
    PUBLIC performance.total_conversions AS SUM(conversions),
    PUBLIC performance.avg_cost_per_click AS AVG(cost_per_click),
    PUBLIC performance.avg_cost_per_acquisition AS AVG(cost_per_acquisition),
    PUBLIC performance.avg_roi AS AVG(roi_percentage),
    PUBLIC performance.avg_engagement_rate AS AVG(engagement_rate),
    PUBLIC campaigns.total_budget AS SUM(budget_allocated),
    PUBLIC campaigns.campaign_count AS COUNT(campaign_id)
  )
  COMMENT = 'Semantic view for analyzing marketing campaign performance and ROI';

-- Verify semantic view was created
SHOW SEMANTIC VIEWS LIKE 'MARKETING_PERFORMANCE_ANALYST';

-- ====================================================================
-- SECTION 7: CREATE CORTEX SEARCH SERVICE
-- ====================================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE MARKETING_CAMPAIGNS_SEARCH
  ON combined_text
  ATTRIBUTES campaign_name, campaign_type, channel, content_type
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 hour'
  AS (
    SELECT 
      c.campaign_id,
      c.campaign_name,
      c.campaign_type,
      c.channel,
      cnt.content_type,
      CONCAT(
        'Campaign: ', c.campaign_name, '. ',
        'Type: ', c.campaign_type, '. ',
        'Channel: ', c.channel, '. ',
        'Description: ', cnt.campaign_description, '. ',
        'Marketing Copy: ', cnt.marketing_copy, '. ',
        'A/B Test Notes: ', cnt.a_b_test_notes
      ) as combined_text
    FROM CAMPAIGNS c
    JOIN CAMPAIGN_CONTENT cnt ON c.campaign_id = cnt.campaign_id
    
    UNION ALL
    
    SELECT 
      c.campaign_id,
      c.campaign_name,
      c.campaign_type,
      c.channel,
      'feedback' as content_type,
      CONCAT(
        'Campaign: ', c.campaign_name, '. ',
        'Customer Segment: ', fb.customer_segment, '. ',
        'Satisfaction Score: ', fb.satisfaction_score, '. ',
        'Comments: ', fb.detailed_comments, '. ',
        'Improvements: ', fb.recommended_improvements
      ) as combined_text
    FROM CAMPAIGNS c
    JOIN CAMPAIGN_FEEDBACK fb ON c.campaign_id = fb.campaign_id
  );

-- Verify search service was created
SHOW CORTEX SEARCH SERVICES LIKE 'MARKETING_CAMPAIGNS_SEARCH';

-- ====================================================================
-- SECTION 8: CREATE REPORT GENERATION STORED PROCEDURE
-- ====================================================================

-- Create an internal stage with directory table enabled
CREATE OR REPLACE STAGE MARKETING_CAMPAIGNS_DB.PUBLIC.CAMPAIGN_REPORTS
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Internal stage to host generated campaign reports';

CREATE OR REPLACE PROCEDURE GENERATE_CAMPAIGN_REPORT_HTML(campaign_id NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER                                                                                                                         
AS
$$
DECLARE
  report_html VARCHAR;
  campaign_info VARCHAR;
  performance_metrics VARCHAR;
  feedback_summary VARCHAR;
  file_name VARCHAR;
  upload_result VARCHAR;
  org_name VARCHAR;
  account_name VARCHAR;
BEGIN
  -- Get campaign basic information
  SELECT 
    '<h1>Campaign Report</h1>' ||
    '<h2>Campaign: ' || campaign_name || '</h2>' ||
    '<p><strong>Type:</strong> ' || campaign_type || '</p>' ||
    '<p><strong>Channel:</strong> ' || channel || '</p>' ||
    '<p><strong>Duration:</strong> ' || start_date || ' to ' || end_date || '</p>' ||
    '<p><strong>Budget:</strong> $' || budget_allocated || '</p>' ||
    '<p><strong>Target Audience:</strong> ' || target_audience || '</p>' ||
    '<p><strong>Status:</strong> ' || status || '</p>'
  INTO campaign_info
  FROM CAMPAIGNS
  WHERE campaign_id = :campaign_id;
  
  -- Get performance metrics summary
  SELECT 
    '<h3>Performance Metrics</h3>' ||
    '<table border="1" style="border-collapse:collapse; width:100%">' ||
    '<tr><th>Metric</th><th>Value</th></tr>' ||
    '<tr><td>Total Impressions</td><td>' || TO_CHAR(SUM(impressions), '999,999,999') || '</td></tr>' ||
    '<tr><td>Total Clicks</td><td>' || TO_CHAR(SUM(clicks), '999,999,999') || '</td></tr>' ||
    '<tr><td>Total Conversions</td><td>' || TO_CHAR(SUM(conversions), '999,999') || '</td></tr>' ||
    '<tr><td>Click-Through Rate</td><td>' || ROUND((SUM(clicks)::FLOAT / SUM(impressions)::FLOAT) * 100, 2) || '%</td></tr>' ||
    '<tr><td>Conversion Rate</td><td>' || ROUND((SUM(conversions)::FLOAT / SUM(clicks)::FLOAT) * 100, 2) || '%</td></tr>' ||
    '<tr><td>Average Cost Per Click</td><td>$' || ROUND(AVG(cost_per_click), 2) || '</td></tr>' ||
    '<tr><td>Average Cost Per Acquisition</td><td>$' || ROUND(AVG(cost_per_acquisition), 2) || '</td></tr>' ||
    '<tr><td>Total Revenue Generated</td><td>$' || TO_CHAR(SUM(revenue_generated), '999,999,999.99') || '</td></tr>' ||
    '<tr><td>Average ROI</td><td>' || ROUND(AVG(roi_percentage), 2) || '%</td></tr>' ||
    '<tr><td>Average Engagement Rate</td><td>' || ROUND(AVG(engagement_rate) * 100, 2) || '%</td></tr>' ||
    '</table>'
  INTO performance_metrics
  FROM CAMPAIGN_PERFORMANCE
  WHERE campaign_id = :campaign_id;
  
  -- Get feedback summary
  SELECT 
    '<h3>Customer Feedback Summary</h3>' ||
    '<p><strong>Average Satisfaction Score:</strong> ' || ROUND(AVG(satisfaction_score), 2) || ' / 5.0</p>' ||
    '<p><strong>Number of Feedback Entries:</strong> ' || COUNT(*) || '</p>' ||
    '<h4>Recent Feedback:</h4>' ||
    LISTAGG(
      '<div style="border:1px solid #ccc; padding:10px; margin:10px 0;">' ||
      '<p><strong>Segment:</strong> ' || customer_segment || '</p>' ||
      '<p><strong>Score:</strong> ' || satisfaction_score || ' / 5.0</p>' ||
      '<p><strong>Comments:</strong> ' || detailed_comments || '</p>' ||
      '<p><strong>Recommendations:</strong> ' || recommended_improvements || '</p>' ||
      '</div>',
      ''
    ) WITHIN GROUP (ORDER BY feedback_date DESC)
  INTO feedback_summary
  FROM CAMPAIGN_FEEDBACK
  WHERE campaign_id = :campaign_id;
  
  -- Combine all sections
  report_html := '<!DOCTYPE html><html><head><style>' ||
    'body { font-family: Arial, sans-serif; margin: 20px; }' ||
    'table { margin: 20px 0; }' ||
    'th { background-color: #4CAF50; color: white; padding: 10px; text-align: left; }' ||
    'td { padding: 10px; }' ||
    'tr:nth-child(even) { background-color: #f2f2f2; }' ||
    '</style></head><body>' ||
    campaign_info ||
    performance_metrics ||
    COALESCE(feedback_summary, '<p>No feedback available</p>') ||
    '<hr><p style="text-align:center; color:#666;">Report Generated: ' || CURRENT_TIMESTAMP() || '</p>' ||
    '</body></html>';
  
  -- Generate filename with timestamp
  file_name := 'CAMPAIGN_' || campaign_id || '_' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD_HH_MI') || '.html';
  
  -- Create a file format for HTML content
  EXECUTE IMMEDIATE '
    CREATE OR REPLACE FILE FORMAT html_format
    TYPE = ''CSV''
    FIELD_DELIMITER = NONE
    RECORD_DELIMITER = NONE
    SKIP_HEADER = 0
    FIELD_OPTIONALLY_ENCLOSED_BY = NONE
    ESCAPE_UNENCLOSED_FIELD = NONE
    COMPRESSION = NONE
    ENCODING = ''UTF8''
  ';
  
  -- Create temporary table to hold the HTML content
  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE temp_report_' || campaign_id || ' (html_content VARCHAR(16777216))';
  
  -- Insert the HTML content
  EXECUTE IMMEDIATE 'INSERT INTO temp_report_' || campaign_id || ' VALUES (?)' USING (report_html);
  
  -- Copy the file to the stage using the HTML file format
  EXECUTE IMMEDIATE 
    'COPY INTO @MARKETING_CAMPAIGNS_DB.PUBLIC.CAMPAIGN_REPORTS/' || file_name || 
    ' FROM (SELECT html_content FROM temp_report_' || campaign_id || ') ' ||
    'FILE_FORMAT = html_format ' ||
    'SINGLE = TRUE OVERWRITE = TRUE HEADER = FALSE';
  
  -- Clean up temporary table
  EXECUTE IMMEDIATE 'DROP TABLE temp_report_' || campaign_id;

SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME() 
  INTO ORG_NAME, ACCOUNT_NAME;
  
  upload_result := 'Report '|| file_name || ' generated and uploaded to stage. View here - https://app.snowflake.com/'|| ORG_NAME ||'/' || ACCOUNT_NAME ||'/#/data/databases/MARKETING_CAMPAIGNS_DB/schemas/PUBLIC/stage/CAMPAIGN_REPORTS';

  
  RETURN upload_result;
END;
$$;

-- Verify procedure was created

SHOW PROCEDURES like 'GENERATE_CAMPAIGN_REPORT_HTML';
-- ====================================================================
-- SECTION 9: TEST THE COMPONENTS
-- ====================================================================

-- Simple campaign performance summary
-- Campaign performance by type


-- Test semantic view
SELECT 
    campaign_type,
    campaign_count,
    total_budget,
    total_revenue,
    avg_roi
FROM SEMANTIC_VIEW(
    MARKETING_PERFORMANCE_ANALYST
    DIMENSIONS campaign_type
    METRICS campaign_count, total_budget, total_revenue, avg_roi
);

-- Test search service
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'MARKETING_CAMPAIGNS_SEARCH',
        '{"query": "email campaigns", "columns": ["campaign_name", "campaign_type"], "limit": 3}'
    )
) as search_results;

-- Test stored procedure
CALL GENERATE_CAMPAIGN_REPORT_HTML(1);

LS @CAMPAIGN_REPORTS;

-- ====================================================================
-- SECTION 10: CREATE CORTEX AGENTS
-- ====================================================================


CREATE OR REPLACE AGENT MARKETING_CAMPAIGN_AGENT_BASELINE
WITH PROFILE='{ "display_name": "MARKETING_CAMPAIGN_AGENT_BASELINE" }'
    COMMENT=$$ Agent specializing in analyzing marketing campaigns for performance, ROI, feedbakc, etc. $$
FROM SPECIFICATION $$
{
    "models": {"orchestration": "auto"},
    "instructions": {
        "orchestration": "",
        "response": "",
        "sample_questions": [
      {
        "question": "What campaigns have the highest ROI?"
      }
    ]
    },
    "tools": [
        {
            "tool_spec": {
                "type": "cortex_analyst_text_to_sql",
                "name": "query_performance_metrics",
                "description": "Query metrics"
            }
        },
        {
            "tool_spec": {
                "type": "cortex_search",
                "name": "search_campaign_content",
                "description": "Search docs"
            }
        },
        {
            "tool_spec": {
                "type": "generic",
                "name": "generate_campaign_report",
                "description": "Create reports",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "campaign_id": {
                            "type": "integer",
                            "description": "The unique identifier of the campaign to generate a report for"
                        }
                    },
                    "required": ["campaign_id"]
                }
            }
        }
    ],
    "tool_resources": {
        "query_performance_metrics": {
            "execution_environment": {
                "query_timeout": 299,
                "type": "warehouse",
                "warehouse": "COMPUTE_WH"
            },
            "semantic_view": "MARKETING_CAMPAIGNS_DB.PUBLIC.MARKETING_PERFORMANCE_ANALYST"
        },
        "search_campaign_content": {
            "execution_environment": {
                "query_timeout": 299,
                "type": "warehouse",
                "warehouse": "COMPUTE_WH"
            },
            "search_service": "MARKETING_CAMPAIGNS_DB.PUBLIC.MARKETING_CAMPAIGNS_SEARCH"
        },
        "generate_campaign_report": {
            "type": "procedure",
            "identifier": "MARKETING_CAMPAIGNS_DB.PUBLIC.GENERATE_CAMPAIGN_REPORT_HTML",
            "execution_environment": {
                "type": "warehouse",
                "warehouse": "COMPUTE_WH",
                "query_timeout": 300
            }
        }
    }
}

$$;

DESCRIBE AGENT MARKETING_CAMPAIGN_AGENT_BASELINE;


CREATE OR REPLACE AGENT MARKETING_CAMPAIGNS_DB.PUBLIC.MARKETING_CAMPAIGN_AGENT_OPTIMIZED
WITH PROFILE='{ "display_name": "MARKETING_CAMPAIGN_AGENT_OPTIMIZED" }'
    COMMENT=$$ Agent specializing in analyzing marketing campaigns for performance, ROI, feedback, etc. $$
FROM SPECIFICATION $$
{
    "models": {"orchestration": "auto"},
    "instructions": {
"orchestration": "You are a marketing campaigns analytics agent with three specialized tools. Follow these STRICT routing rules to ensure consistent tool selection:

## TOOL ROUTING RULES (Apply in order)


### Rule 1: Quantitative Analysis (Use query_performance_metrics)
Use query_performance_metrics when the query involves:
- NUMERICAL METRICS: revenue, ROI, conversions, clicks, impressions, costs, budget, engagement rates
- CALCULATIONS: totals, averages, percentages, ratios, growth rates, trends over time
- COMPARISONS: top/bottom campaigns, ranking, channel comparison, time period analysis
- AGGREGATIONS: sum, count, average, min, max by dimensions like channel, type, audience
- PERFORMANCE QUESTIONS: 'how much', 'how many', 'what is the rate', 'calculate'
- Keywords: 'revenue', 'ROI', 'cost', 'conversions', 'clicks', 'performance', 'metrics', 'total', 'average', 'rate', 'top', 'bottom', 'best', 'worst', 'compare'
- Examples: 'What was total revenue by channel?', 'Which campaigns had highest ROI?', 'Show me conversion rates over time', 'Compare email vs social performance'

### Rule 3: Qualitative Analysis (Use search_campaign_content)
Use search_campaign_content when the query involves:
- TEXT CONTENT: campaign descriptions, marketing copy, messaging, creative elements
- CUSTOMER FEEDBACK: comments, reviews, satisfaction, sentiment, recommendations
- STRATEGY INSIGHTS: A/B testing notes, tactics, approaches, best practices, lessons learned
- CONTENT DISCOVERY: finding campaigns by theme, approach, or content similarity
- QUALITATIVE QUESTIONS: 'what did customers say', 'what was the strategy', 'find campaigns about'
- Keywords: 'feedback', 'comments', 'description', 'copy', 'content', 'strategy', 'A/B test', 'customer said', 'testimonials', 'improvements', 'about', 'similar to', 'messaging'
- Examples: 'What feedback did we get on email campaigns?', 'Find campaigns about sustainability', 'What was the messaging strategy?', 'Show A/B test insights'

### Rule 4: Report Generation 
** Always use query_performance_metrics tool first to determine campaign_ID to pass in to report generate_campaign_report tool **
Use generate_campaign_report when:
- User explicitly requests a 'report' or 'comprehensive report'
- User asks to 'generate', 'create', or 'show' a report
- User provides or mentions a campaign_id and wants detailed information
- Keywords: 'report', 'HTML', 'full details', 'comprehensive analysis'
- Examples: 'Generate a report for campaign 5', 'Create report for Spring Fashion Launch'
** Always share insights with the user immediately upon creating the report - rather than simply creating the report itself . This can be done with one more additional call to query_performance_metrics**

### Rule 4: Multi-Tool Queries
For queries needing BOTH quantitative AND qualitative data:
1. FIRST use query_performance_metrics for numerical data
2. THEN use search_campaign_content for qualitative insights
3. Combine results in your response
- Examples: 'Analyze our best performing campaign' (metrics + strategy), 'What made the Spring campaign successful?' (ROI + feedback)

## CONSISTENCY REQUIREMENTS
- For identical queries, ALWAYS use the same tool(s)
- If a query contains both metric keywords AND content keywords, default to query_performance_metrics
- If campaign_id is provided without explicit report request, use query_performance_metrics to filter by that campaign
- Never use search_campaign_content for numerical analysis
- Never use query_performance_metrics for text content or feedback

## WHEN UNCERTAIN
If the query is ambiguous:
1. Check for numerical keywords → use query_performance_metrics
2. Check for content keywords → use search_campaign_content
3. If still unclear, ask the user to clarify whether they want metrics or content insights",                                                                                                                                                                                     
                                                                                                                                                                                                
     "response": "Follow these response formatting rules for consistency:\n\n1. STRUCTURE:\n   - Start with a direct answer to the question\n   - Present data in clear, organized format       
 (tables, lists, or sections)\n   - End with actionable insights or recommendations\n\n2. METRICS PRESENTATION:\n   - Always include units (dollars, percentages, counts)\n   - Format large    
 numbers with commas (e.g., 1,234,567)\n   - Round percentages to 2 decimal places\n   - Provide context (e.g., 'X% higher than average')\n\n3. CONTENT SUMMARIZATION:\n   - Quote key phrases  
 from original content\n   - Identify themes across multiple results\n   - Highlight actionable recommendations\n   - Mention specific campaign names when relevant\n\n4. CITATIONS:\n   -      
 Always cite specific campaigns by name\n   - Include dates when discussing time-based data\n   - Reference specific metrics by name\n\n5. TONE:\n   - Professional and data-driven\n   -       
 Concise but complete\n   - Actionable and insight-focused\n   - Avoid speculation; base all statements on data\n\n6. CONSISTENCY:\n   - Use the same format for similar queries\n   - Present  
 metrics in the same order (revenue, ROI, conversions, etc.)\n   - Use consistent terminology (e.g., always 'ROI' not 'return on investment')"          
    },
    "tools": [
        {
            "tool_spec": {
                "type": "cortex_analyst_text_to_sql",
                "name": "query_performance_metrics",
                "description": "Query structured performance data including campaign ROI, revenue, budget efficiency, impressions, clicks, conversions, cost metrics, and engagement rates. Use for quantitative analysis of campaign performance across channels and time periods."
            }
        },
        {
            "tool_spec": {
                "type": "cortex_search",
                "name": "search_campaign_content",
                "description": "Search unstructured campaign content including campaign descriptions, marketing copy, A/B test results, customer feedback, and recommended improvements. Use for qualitative insights, content discovery, and learning from past campaigns."
            }
        },
        {
            "tool_spec": {
                "type": "generic",
                "name": "generate_campaign_report",
                "description": "Generate a comprehensive HTML report for a specific campaign including all performance metrics, customer feedback, and key insights. Returns formatted report ready for PDF conversion.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "campaign_id": {
                            "type": "integer",
                            "description": "The unique identifier of the campaign to generate a report for"
                        }
                    },
                    "required": ["campaign_id"]
                }
            }
        }
    ],
    "tool_resources": {
        "query_performance_metrics": {
            "execution_environment": {
                "query_timeout": 299,
                "type": "warehouse",
                "warehouse": "COMPUTE_WH"
            },
            "semantic_view": "MARKETING_CAMPAIGNS_DB.PUBLIC.MARKETING_PERFORMANCE_ANALYST"
        },
        "search_campaign_content": {
            "execution_environment": {
                "query_timeout": 299,
                "type": "warehouse",
                "warehouse": "COMPUTE_WH"
            },
            "search_service": "MARKETING_CAMPAIGNS_DB.PUBLIC.MARKETING_CAMPAIGNS_SEARCH"
        },
        "generate_campaign_report": {
            "type": "procedure",
            "identifier": "MARKETING_CAMPAIGNS_DB.PUBLIC.GENERATE_CAMPAIGN_REPORT_HTML",
            "execution_environment": {
                "type": "warehouse",
                "warehouse": "COMPUTE_WH",
                "query_timeout": 300
            }
        }
    }
}
$$;


DESCRIBE AGENT MARKETING_CAMPAIGN_AGENT_OPTIMIZED;



-- ====================================================================
-- SECTION 11: CREATE EVALUATION DATASET
-- ====================================================================

-- ADD code to bring data in from git repo


$$
====================================================================
MARKETING CAMPAIGNS ANALYTICS SYSTEM - SETUP COMPLETE
====================================================================

✅ Database created: MARKETING_CAMPAIGNS_DB' 
'✅ Tables created with sample data:
   - CAMPAIGNS (25 records)
   - CAMPAIGN_PERFORMANCE (1,578 records)
   - CAMPAIGN_CONTENT (25 records)
   - CAMPAIGN_FEEDBACK (23 records)

✅ Semantic View created: MARKETING_PERFORMANCE_ANALYST
✅ Cortex Search Service created: MARKETING_CAMPAIGNS_SEARCH
✅ Stored Procedure created: GENERATE_CAMPAIGN_REPORT_PDF
✅ Baseline Agent Created: MARKETING_CAMPAIGN_AGENT_BASELINE
✅ Optimized Agent created: MARKETING_CAMPAIGN_AGENT_OPTIMIZED

EXAMPLE QUERIES:
- "What are the top 5 campaigns by ROI?"
- "What feedback did customers give about email campaigns?"
- "Generate a report for campaign ID 1"

Now follow the below instructions to evaluate the performance of each agent!

- Navigate to your newly created agent and click into Evaluations Tab
    - Name your new evaluation run and optionally give a description [click next]
    - Select Create New Dataset
        - Select MARKETING_CAMPAIGNS_DB.PUBLIC.EVALS_TABLE as your input table
        - Select MARKETING_CAMPAIGNS_DB.PUBLIC.QUICKSTART_EVALSET as your new dataset destination [click next]
    - Select INPUT_QUERY as your Query Text column
        - Check boxes for all metrics available
        - Tool Selection Accuracy, Tool Execution Accuracy, and Answer Correctness should reference the EXPECTED_TOOLS column
        - Click Create Evaluation
        
Now wait as your queries are executed and your evaluation metrics are computed! This should populate in roughly ~3-5 minutes.

Compare how the baseline agent and the optimized agent performed on various metrics!

====================================================================
$$ as setup_status;
